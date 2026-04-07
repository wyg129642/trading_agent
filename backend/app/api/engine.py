"""Engine control API: status, start, stop, restart, logs."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.app.deps import get_current_user, get_current_admin
from backend.app.models.user import User

router = APIRouter()


class EngineStatusResponse(BaseModel):
    is_running: bool
    pid: int | None = None
    start_time: str | None = None
    uptime_seconds: int | None = None
    restart_count: int = 0
    auto_restart: bool = True
    engine_status: str = "unknown"
    engine_message: str = ""
    monitors: int = 0
    queue_size: int = 0
    stats: dict | None = None
    last_heartbeat: str | None = None


class EngineActionResponse(BaseModel):
    success: bool
    message: str


def _get_engine_manager(request: Request):
    mgr = getattr(request.app.state, "engine_manager", None)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Engine manager not initialized")
    return mgr


@router.get("/status", response_model=EngineStatusResponse)
async def get_engine_status(
    request: Request,
    user: User = Depends(get_current_user),
):
    """Get current engine status and health."""
    mgr = _get_engine_manager(request)
    return EngineStatusResponse(**mgr.get_health())


@router.post("/start", response_model=EngineActionResponse)
async def start_engine(
    request: Request,
    user: User = Depends(get_current_admin),
):
    """Start the trading engine (admin only)."""
    mgr = _get_engine_manager(request)
    if mgr.is_running:
        return EngineActionResponse(success=False, message="Engine is already running")
    ok = await mgr.start()
    return EngineActionResponse(
        success=ok,
        message="Engine started" if ok else "Failed to start engine",
    )


@router.post("/stop", response_model=EngineActionResponse)
async def stop_engine(
    request: Request,
    user: User = Depends(get_current_admin),
):
    """Stop the trading engine (admin only)."""
    mgr = _get_engine_manager(request)
    if not mgr.is_running:
        return EngineActionResponse(success=False, message="Engine is not running")
    ok = await mgr.stop()
    return EngineActionResponse(
        success=ok,
        message="Engine stopped" if ok else "Failed to stop engine",
    )


@router.post("/restart", response_model=EngineActionResponse)
async def restart_engine(
    request: Request,
    user: User = Depends(get_current_admin),
):
    """Restart the trading engine (admin only)."""
    mgr = _get_engine_manager(request)
    ok = await mgr.restart()
    return EngineActionResponse(
        success=ok,
        message="Engine restarted" if ok else "Failed to restart engine",
    )


@router.get("/logs")
async def get_engine_logs(
    request: Request,
    lines: int = 100,
    user: User = Depends(get_current_user),
):
    """Get recent engine log lines."""
    mgr = _get_engine_manager(request)
    return {"logs": mgr.get_recent_logs(lines)}


@router.post("/auto-restart", response_model=EngineActionResponse)
async def toggle_auto_restart(
    request: Request,
    enable: bool = True,
    user: User = Depends(get_current_admin),
):
    """Enable/disable auto-restart on crash (admin only)."""
    mgr = _get_engine_manager(request)
    if enable:
        await mgr.enable_auto_restart()
    else:
        await mgr.disable_auto_restart()
    return EngineActionResponse(
        success=True,
        message=f"Auto-restart {'enabled' if enable else 'disabled'}",
    )
