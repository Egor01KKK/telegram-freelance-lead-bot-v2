from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_PROFILE: dict[str, Any] = {
    "name": "Фрилансер",
    "positioning": "Помогаю быстро и аккуратно делать технические задачи под бизнес.",
    "services": [],
    "stack": [],
    "cases": [],
    "tone": "Коротко, по делу, без давления и без обещаний, которых нет в профиле.",
    "do_not_claim": [
        "Не выдумывать опыт, цифры, кейсы, клиентов, сроки и гарантии.",
        "Если данных не хватает, задать уточняющий вопрос.",
    ],
    "avoid_projects": [],
}


def load_freelancer_profile(path: Path) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_PROFILE

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Freelancer profile must be a JSON object")

    profile = dict(DEFAULT_PROFILE)
    profile.update(data)
    return profile
