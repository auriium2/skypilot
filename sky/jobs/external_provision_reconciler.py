"""Reconcile durable managed-job provider queues without launch workers."""
import random
import time

from sky import sky_logging
from sky.adaptors import gcp
from sky.jobs import external_provision
from sky.utils import locks

logger = sky_logging.init_logger(__name__)

_POLL_INITIAL_SECONDS = 10
_POLL_MAX_SECONDS = 120


def _resource(record):
    return gcp.build(
        'tpu', 'v2', credentials=None,
        discoveryServiceUrl='https://tpu.googleapis.com/$discovery/rest')


def _name(record):
    return (f'projects/{record["project"]}/locations/{record["zone"]}/'
            f'queuedResources/{record["provider_resource_id"]}')


def _not_found(error) -> bool:
    status = getattr(error, 'status_code', None)
    if status is None:
        status = getattr(getattr(error, 'resp', None), 'status', None)
    return status == 404


def _backoff(attempt: int) -> float:
    base = min(_POLL_MAX_SECONDS,
               _POLL_INITIAL_SECONDS * (2**min(attempt, 4)))
    return base * random.uniform(0.8, 1.2)


def _delete(record, service) -> None:
    try:
        operation = service.projects().locations().queuedResources().delete(
            name=_name(record), force=True).execute()
        logger.info('Deleting queued TPU request %s: %s',
                    record['provider_resource_id'], operation.get('name'))
    except gcp.http_error_exception() as e:
        if not _not_found(e):
            raise


def reconcile_record(record) -> None:
    if record['provider'] != 'gcp_tpu_queued_resource':
        return
    service = _resource(record)
    if record['state'] == (
            external_provision.ExternalProvisionState.CANCELLING.value):
        if record.get('provider_state') != 'DELETING':
            _delete(record, service)
        try:
            current = service.projects().locations().queuedResources().get(
                name=_name(record)).execute()
        except gcp.http_error_exception() as e:
            if _not_found(e):
                target = (external_provision.ExternalProvisionState.FAILED.value
                          if record.get('last_error_code') else
                          external_provision.ExternalProvisionState.CANCELLED.
                          value)
                external_provision.update(
                    record['id'],
                    local_state=target,
                    provider_state='ABSENT')
                return
            raise
        provider_state = current.get('state', {}).get('state', 'DELETING')
        external_provision.update(
            record['id'], provider_state=provider_state,
            next_poll_at=time.time() + _backoff(0),
            poll_attempt=0)
        return

    try:
        provider = service.projects().locations().queuedResources().get(
            name=_name(record)).execute()
    except gcp.http_error_exception() as e:
        if _not_found(e) and record['state'] == (
                external_provision.ExternalProvisionState.SUBMITTING.value):
            # Submission owner may have crashed before the create call.  The
            # next managed launch re-enters idempotently and performs it.
            external_provision.update(
                record['id'], next_poll_at=time.time() + _backoff(
                    record['poll_attempt']),
                poll_attempt=record['poll_attempt'] + 1)
            return
        if _not_found(e):
            external_provision.update(
                record['id'],
                local_state=external_provision.ExternalProvisionState.FAILED.
                value,
                provider_state='ABSENT', error_code='PROVIDER_RESOURCE_LOST',
                error_message='Queued TPU request disappeared before '
                'becoming active.')
            return
        raise

    state = provider.get('state', {}).get('state', 'STATE_UNSPECIFIED')
    if state == 'ACTIVE':
        if record['state'] == (
                external_provision.ExternalProvisionState.FINALIZING.value):
            external_provision.update(
                record['id'], provider_state=state,
                next_poll_at=time.time() + _POLL_MAX_SECONDS,
                poll_attempt=0)
            return
        external_provision.update(
            record['id'],
            local_state=external_provision.ExternalProvisionState.ACTIVE.value,
            provider_state=state, next_poll_at=time.time() + _POLL_MAX_SECONDS,
            poll_attempt=0)
        return
    if state == 'FAILED':
        status = provider.get('status', {})
        _delete(record, service)
        external_provision.update(
            record['id'],
            local_state=external_provision.ExternalProvisionState.CANCELLING.
            value,
            provider_state=state,
            error_code=str(status.get('code', 'PROVIDER_FAILED')),
            error_message=str(status.get('message', status)))
        return
    if state == 'SUSPENDED':
        external_provision.update(
            record['id'],
            local_state=external_provision.ExternalProvisionState.EXPIRED.value,
            provider_state=state, error_code='PROVIDER_SUSPENDED',
            error_message='The TPU queue window or provider-managed lifetime '
            'ended before the workload completed.')
        return
    if state in ('DELETING', 'SUSPENDING'):
        external_provision.update(
            record['id'], provider_state=state,
            next_poll_at=time.time() + _backoff(0), poll_attempt=0)
        return
    local_state = (external_provision.ExternalProvisionState.PROVISIONING.value
                   if state == 'PROVISIONING' else
                   external_provision.ExternalProvisionState.QUEUED.value)
    attempt = (0 if state != record.get('provider_state') else
               record['poll_attempt'] + 1)
    external_provision.update(
        record['id'], local_state=local_state, provider_state=state,
        next_poll_at=time.time() + _backoff(attempt), poll_attempt=attempt)


def reconcile_once(limit: int = 100) -> int:
    lock = locks.get_lock('external-provision-reconciler', timeout=0)
    try:
        with lock:
            records = external_provision.list_due(limit)
            for record in records:
                try:
                    reconcile_record(record)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        'External provision reconcile failed for %s: %s',
                        record['id'], e)
                    external_provision.update(
                        record['id'],
                        next_poll_at=time.time() + _POLL_MAX_SECONDS,
                        poll_attempt=record['poll_attempt'] + 1,
                        error_code='RECONCILE_ERROR', error_message=str(e))
            return len(records)
    except locks.LockTimeout:
        return 0
