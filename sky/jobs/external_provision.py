"""Durable provider-queue ownership for managed jobs."""
import enum
import json
import time
from typing import Any, Dict, List, Optional
import uuid

import sqlalchemy
from sqlalchemy import orm

from sky.jobs import state
from sky.utils.db import retries as db_retries


class ExternalProvisionState(enum.Enum):
    SUBMITTING = 'SUBMITTING'
    QUEUED = 'QUEUED'
    PROVISIONING = 'PROVISIONING'
    ACTIVE = 'ACTIVE'
    FINALIZING = 'FINALIZING'
    COMPLETE = 'COMPLETE'
    CANCELLING = 'CANCELLING'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'
    EXPIRED = 'EXPIRED'


TERMINAL_STATES = {
    ExternalProvisionState.COMPLETE.value,
    ExternalProvisionState.CANCELLED.value,
    ExternalProvisionState.FAILED.value,
    ExternalProvisionState.EXPIRED.value,
}


def _row_dict(row) -> Optional[Dict[str, Any]]:
    return None if row is None else dict(row._mapping)


@db_retries.retry
def get(job_id: int, task_id: int, attempt: int) -> Optional[Dict[str, Any]]:
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                sqlalchemy.and_(
                    state.external_provisions_table.c.job_id == job_id,
                    state.external_provisions_table.c.task_id == task_id,
                    state.external_provisions_table.c.attempt == attempt,
                ))).fetchone()
        return _row_dict(row)


@db_retries.retry
def get_by_id(record_id: str) -> Optional[Dict[str, Any]]:
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                state.external_provisions_table.c.id == record_id)).fetchone()
        return _row_dict(row)


@db_retries.retry
def get_by_cluster(cluster_name_on_cloud: str) -> Optional[Dict[str, Any]]:
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                state.external_provisions_table.c.cluster_name_on_cloud ==
                cluster_name_on_cloud).order_by(
                    state.external_provisions_table.c.attempt.desc())).fetchone()
        return _row_dict(row)


@db_retries.retry
def get_latest(job_id: int, task_id: int) -> Optional[Dict[str, Any]]:
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                sqlalchemy.and_(
                    state.external_provisions_table.c.job_id == job_id,
                    state.external_provisions_table.c.task_id == task_id,
                )).order_by(
                    state.external_provisions_table.c.attempt.desc())).fetchone()
        return _row_dict(row)


@db_retries.retry
def create_or_get(*, job_id: int, task_id: int, attempt: int,
                  cluster_name: str, cluster_name_on_cloud: str, provider: str,
                  project: str, region: str, zone: str,
                  provider_resource_id: str, request_hash: str,
                  request: Dict[str, Any]) -> Dict[str, Any]:
    existing = get(job_id, task_id, attempt)
    if existing is not None:
        if (existing['request_hash'] != request_hash or
                existing['provider_resource_id'] != provider_resource_id):
            raise RuntimeError('Existing external provision does not match '
                               'the current request; refusing unsafe adoption.')
        return existing
    now = time.time()
    values = dict(
        id=str(uuid.uuid4()), job_id=job_id, task_id=task_id, attempt=attempt,
        cluster_name=cluster_name,
        cluster_name_on_cloud=cluster_name_on_cloud, provider=provider,
        project=project, region=region, zone=zone,
        provider_resource_id=provider_resource_id,
        request_hash=request_hash,
        request_json=json.dumps(request, sort_keys=True, separators=(',', ':')),
        state=ExternalProvisionState.SUBMITTING.value,
        created_at=now, updated_at=now, next_poll_at=now, poll_attempt=0)
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    try:
        with orm.Session(engine) as session:
            session.execute(sqlalchemy.insert(
                state.external_provisions_table).values(**values))
            session.commit()
    except sqlalchemy.exc.IntegrityError:
        existing = get(job_id, task_id, attempt)
        if existing is None:
            raise
        if existing['request_hash'] != request_hash:
            raise RuntimeError('Concurrent external provision request mismatch.')
        return existing
    return values


@db_retries.retry
def update(record_id: str, *, local_state: Optional[str] = None,
           provider_state: Optional[str] = None, next_poll_at: Optional[float] = None,
           poll_attempt: Optional[int] = None, error_code: Optional[str] = None,
           error_message: Optional[str] = None) -> None:
    now = time.time()
    values: Dict[str, Any] = {'updated_at': now}
    if local_state is not None:
        values['state'] = local_state
        if local_state == ExternalProvisionState.ACTIVE.value:
            values['active_at'] = now
        if local_state in TERMINAL_STATES:
            values['terminal_at'] = now
    if provider_state is not None:
        values['provider_state'] = provider_state
    if next_poll_at is not None:
        values['next_poll_at'] = next_poll_at
    if poll_attempt is not None:
        values['poll_attempt'] = poll_attempt
    if error_code is not None:
        values['last_error_code'] = error_code
    if error_message is not None:
        values['last_error_message'] = error_message[:2048]
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.external_provisions_table).where(
                state.external_provisions_table.c.id == record_id).values(values))
        session.commit()


def complete_by_cluster(cluster_name_on_cloud: str) -> None:
    record = get_by_cluster(cluster_name_on_cloud)
    if record is not None and record['state'] == (
            ExternalProvisionState.FINALIZING.value):
        update(record['id'], local_state=ExternalProvisionState.COMPLETE.value,
               provider_state='ACTIVE')


@db_retries.retry
def list_due(limit: int = 100) -> List[Dict[str, Any]]:
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                sqlalchemy.and_(
                    sqlalchemy.not_(state.external_provisions_table.c.state.in_(
                        TERMINAL_STATES)),
                    state.external_provisions_table.c.next_poll_at <= time.time(),
                )).order_by(
                    state.external_provisions_table.c.next_poll_at).limit(limit)
        ).fetchall()
        return [dict(row._mapping) for row in rows]


@db_retries.retry
def get_waiting_details(job_ids: List[int]) -> Dict[int, str]:
    if not job_ids:
        return {}
    active_states = [
        ExternalProvisionState.SUBMITTING.value,
        ExternalProvisionState.QUEUED.value,
        ExternalProvisionState.PROVISIONING.value,
        ExternalProvisionState.ACTIVE.value,
        ExternalProvisionState.FINALIZING.value,
    ]
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(state.external_provisions_table).where(
                sqlalchemy.and_(
                    state.external_provisions_table.c.job_id.in_(job_ids),
                    state.external_provisions_table.c.state.in_(active_states)))
        ).fetchall()
    details = {}
    for row in rows:
        record = dict(row._mapping)
        request = json.loads(record['request_json'])
        model = request.get('provisioningModel', 'QUEUED')
        waited = max(0, int(time.time() - record['created_at']))
        details[record['job_id']] = (
            f'Waiting for TPU capacity ({model}, {record["zone"]}, '
            f'{waited}s; provider state {record.get("provider_state") or "-"})')
    return details


@db_retries.retry
def request_cancel(job_id: int, task_id: Optional[int] = None) -> None:
    now = time.time()
    conditions = [state.external_provisions_table.c.job_id == job_id]
    if task_id is not None:
        conditions.append(state.external_provisions_table.c.task_id == task_id)
    engine = state._db_manager.get_engine()  # pylint: disable=protected-access
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.external_provisions_table).where(
                sqlalchemy.and_(*conditions,
                    sqlalchemy.not_(state.external_provisions_table.c.state.in_([
                        ExternalProvisionState.CANCELLED.value,
                        ExternalProvisionState.FAILED.value,
                        ExternalProvisionState.EXPIRED.value,
                    ])))).values(
                            state=ExternalProvisionState.CANCELLING.value,
                            cancel_requested_at=now, updated_at=now,
                            next_poll_at=now))
        session.commit()


def wait_for_job_cleanup(job_id: int, timeout: float = 180.0) -> None:
    """Wait until every external provision owned by a job is terminal."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        engine = state._db_manager.get_engine()  # pylint: disable=protected-access
        with orm.Session(engine) as session:
            remaining = session.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    state.external_provisions_table).where(
                        sqlalchemy.and_(
                            state.external_provisions_table.c.job_id == job_id,
                            sqlalchemy.not_(
                                state.external_provisions_table.c.state.in_(
                                    TERMINAL_STATES))))).scalar_one()
        if remaining == 0:
            return
        time.sleep(2)
    raise TimeoutError('Timed out cleaning up queued TPU capacity request.')
