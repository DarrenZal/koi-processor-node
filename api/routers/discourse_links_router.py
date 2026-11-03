"""Discourse-link read and review endpoints (prototype). NOT MOUNTED.

Nothing includes this router yet; mounting it is an activation step. It has no
discovery route and no way to reach fixture-replay authority.

Authority is built here, server-side. The credential dependency requires a
configured admin token (KOI_ADMIN_TOKEN or KOI_STATE_DIR/admin_token) and the
existing localhost-plus-token check (api.auth.enforce_local_admin). The actor
recorded on a judgment comes from server config (KOI_DISCOURSE_OPERATOR_ACTOR),
never from the request: request bodies reject unknown fields, so a caller cannot
send an actor or basis at all.

This is privileged local-credential authorization (basis
privileged-local-credential), not proof that a particular person approved a
judgment: any local process that can read the token passes it. Judgment
endpoints also stay disabled unless KOI_DISCOURSE_JUDGMENTS=1.

Agents may propose labels from localhost without the token. A proposal is
stored as caller-declared and never counted.
"""

from __future__ import annotations

import json
import os
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from api import discourse_links as dl
from api.auth import enforce_local_admin, read_admin_token

LOCAL_HOSTS = ("127.0.0.1", "::1", "localhost")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProposalBody(_Strict):
    relation: str
    semantics: str
    rationale: str = Field(min_length=1)
    proposed_by: str = Field(min_length=1)


class ReviewBody(_Strict):
    decision: Literal["admit", "decline"]
    rationale: str = Field(min_length=1)
    relation: Optional[str] = None
    semantics: Optional[str] = None
    reverse: bool = False


class RationaleBody(_Strict):
    rationale: str = Field(min_length=1)


class CorrectionBody(_Strict):
    rationale: str = Field(min_length=1)
    relation: str
    semantics: str
    reverse: bool = False


def require_operator(request: Request) -> dl.OperatorPrincipal:
    """A caller holding the privileged local credential, or an HTTP error."""
    if not read_admin_token():
        raise HTTPException(status_code=503, detail=(
            "operator endpoints are disabled until an admin token is configured "
            "(KOI_ADMIN_TOKEN or KOI_STATE_DIR/admin_token)"))
    err = enforce_local_admin(request)
    if err is not None:
        raise HTTPException(status_code=err.status_code, detail=json.loads(err.body)["error"])
    return dl._mint_operator_principal(
        actor=os.getenv("KOI_DISCOURSE_OPERATOR_ACTOR", "operator:personal"),
        authenticated_by="local-admin-token")


def require_local(request: Request) -> str:
    host = request.client.host if request.client else None
    if host not in LOCAL_HOSTS:
        raise HTTPException(status_code=403, detail="proposals are accepted from localhost only")
    return host


def create_discourse_links_router(pool) -> APIRouter:
    router = APIRouter(prefix="/discourse-links", tags=["discourse-links"])

    async def call(fn, *args, **kwargs):
        async with pool.acquire() as conn:
            try:
                return await fn(conn, *args, **kwargs)
            except dl.NotVisible:
                raise HTTPException(status_code=404, detail="move not found")
            except dl.ReviewRefused as exc:
                raise HTTPException(status_code=409, detail=str(exc))

    @router.get("/moves/{move_id}/connected")
    async def connected(move_id: str, include_candidates: bool = False,
                        _op: dl.OperatorPrincipal = Depends(require_operator)):
        return await call(dl.connected_material, move_id, context=dl.personal_context(),
                          include_candidates=include_candidates)

    @router.get("/moves/{move_id}/corroboration")
    async def corroboration(move_id: str, _op: dl.OperatorPrincipal = Depends(require_operator)):
        return await call(dl.corroboration_report, move_id, context=dl.personal_context())

    @router.get("/coverage")
    async def coverage(model: Optional[str] = None, _op: dl.OperatorPrincipal = Depends(require_operator)):
        return await call(dl.retrieval_coverage, model, context=dl.personal_context())

    @router.get("/same-work")
    async def same_work(_op: dl.OperatorPrincipal = Depends(require_operator)):
        return await call(dl.same_work_signals, context=dl.personal_context())

    @router.post("/candidates/{candidate_id}/proposals")
    async def propose(candidate_id: str, body: ProposalBody, _host: str = Depends(require_local)):
        await call(dl.propose_relation, candidate_id, relation=body.relation, semantics=body.semantics,
                   proposed_by=f"declared:{body.proposed_by}", rationale=body.rationale)
        return {"candidate_id": candidate_id, "status": "candidate", "proposal": "recorded, not counted"}

    @router.post("/candidates/{candidate_id}/review")
    async def review(candidate_id: str, body: ReviewBody,
                     op: dl.OperatorPrincipal = Depends(require_operator)):
        rel_id = await call(dl.review_candidate, candidate_id, decision=body.decision,
                            authority=dl.mint_operator_authority(op), rationale=body.rationale,
                            relation=body.relation, semantics=body.semantics, reverse=body.reverse)
        return {"candidate_id": candidate_id, "decision": body.decision, "relation_id": rel_id,
                "asserted_by": op.actor, "authority_basis": dl.LOCAL_CREDENTIAL}

    @router.post("/candidates/{candidate_id}/reopen")
    async def reopen(candidate_id: str, body: RationaleBody,
                     op: dl.OperatorPrincipal = Depends(require_operator)):
        event_id = await call(dl.reopen_candidate, candidate_id, authority=dl.mint_operator_authority(op),
                              rationale=body.rationale)
        return {"candidate_id": candidate_id, "event_id": event_id}

    @router.post("/relations/{relation_id}/withdraw")
    async def withdraw(relation_id: str, body: RationaleBody,
                       op: dl.OperatorPrincipal = Depends(require_operator)):
        event_id = await call(dl.withdraw_relation, relation_id, authority=dl.mint_operator_authority(op),
                              rationale=body.rationale)
        return {"relation_id": relation_id, "event_id": event_id}

    @router.post("/relations/{relation_id}/correct")
    async def correct(relation_id: str, body: CorrectionBody,
                      op: dl.OperatorPrincipal = Depends(require_operator)):
        new_id = await call(dl.correct_relation, relation_id, authority=dl.mint_operator_authority(op),
                            rationale=body.rationale, relation=body.relation, semantics=body.semantics,
                            reverse=body.reverse)
        return {"relation_id": relation_id, "replaced_by": new_id}

    return router
