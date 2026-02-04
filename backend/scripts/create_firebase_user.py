from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass

import firebase_admin
from firebase_admin import auth, credentials, db


@dataclass
class UserInput:
    email: str
    password: str | None
    first_name: str | None
    last_name: str | None
    role: str
    env: str
    must_change_password: bool
    email_verified: bool
    uid: str | None
    update_existing: bool
    roles_only: bool


def _ensure_firebase_app() -> None:
    firebase_json = os.environ.get("ACTUS_FIREBASE_JSON")
    firebase_path = os.environ.get("ACTUS_FIREBASE_PATH")

    if firebase_json:
        firebase_config = json.loads(firebase_json)
    elif firebase_path:
        with open(firebase_path, "r") as handle:
            firebase_config = json.load(handle)
    else:
        raise RuntimeError(
            "Missing Firebase credentials. Set ACTUS_FIREBASE_JSON or ACTUS_FIREBASE_PATH."
        )

    if "private_key" in firebase_config and "\\n" in firebase_config["private_key"]:
        firebase_config["private_key"] = firebase_config["private_key"].replace("\\n", "\n")

    database_url = os.environ.get(
        "ACTUS_FIREBASE_URL", "https://creditapp-tm-default-rtdb.firebaseio.com/"
    )

    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_config)
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": database_url},
        )


def _display_name(user_input: UserInput) -> str | None:
    if not user_input.first_name and not user_input.last_name:
        return None
    if user_input.first_name and user_input.last_name:
        return f"{user_input.first_name} {user_input.last_name}"
    return user_input.first_name or user_input.last_name


def _create_or_update_auth_user(user_input: UserInput) -> auth.UserRecord:
    display_name = _display_name(user_input)
    if user_input.uid:
        if not user_input.password and not user_input.update_existing:
            return auth.get_user(user_input.uid)
        return auth.update_user(
            user_input.uid,
            email=user_input.email,
            password=user_input.password,
            display_name=display_name,
            email_verified=user_input.email_verified,
        )

    try:
        return auth.create_user(
            email=user_input.email,
            password=user_input.password or "",
            display_name=display_name,
            email_verified=user_input.email_verified,
        )
    except auth.EmailAlreadyExistsError:
        if not user_input.update_existing:
            raise
        user = auth.get_user_by_email(user_input.email)
        return auth.update_user(
            user.uid,
            password=user_input.password,
            display_name=display_name,
            email_verified=user_input.email_verified,
        )


def _write_role_record(user: auth.UserRecord, user_input: UserInput) -> None:
    payload = {
        "email": user_input.email,
        "env": user_input.env,
        "firstName": user_input.first_name or "",
        "lastName": user_input.last_name or "",
        "mustChangePassword": bool(user_input.must_change_password),
        "role": user_input.role,
        "updatedAt": int(time.time() * 1000),
    }
    db.reference("user_roles").child(user.uid).set(payload)


def _parse_args() -> UserInput:
    parser = argparse.ArgumentParser(
        description="Create or update a Firebase Auth user and user_roles entry."
    )
    parser.add_argument("--email", required=True, help="User email address.")
    parser.add_argument(
        "--password",
        help="Password for the user (required for new users unless --update-existing).",
    )
    parser.add_argument("--first-name", help="First name.")
    parser.add_argument("--last-name", help="Last name.")
    parser.add_argument("--role", required=True, help="Role value to store in user_roles.")
    parser.add_argument(
        "--env",
        default=os.environ.get("ACTUS_ENV", "prod"),
        help="Environment label to store in user_roles (default: ACTUS_ENV or prod).",
    )
    parser.add_argument(
        "--must-change-password",
        action="store_true",
        help="Set mustChangePassword=true in user_roles.",
    )
    parser.add_argument(
        "--email-verified",
        action="store_true",
        help="Mark email as verified in Firebase Auth.",
    )
    parser.add_argument("--uid", help="Existing Firebase Auth UID to update.")
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="If email exists, update that user instead of failing.",
    )
    parser.add_argument(
        "--roles-only",
        action="store_true",
        help="Only write user_roles (no Auth create/update). Requires --uid or an existing email.",
    )

    args = parser.parse_args()

    if not args.password and not args.update_existing and not args.uid and not args.roles_only:
        raise SystemExit("--password is required for new users.")

    return UserInput(
        email=args.email,
        password=args.password,
        first_name=args.first_name,
        last_name=args.last_name,
        role=args.role,
        env=args.env,
        must_change_password=bool(args.must_change_password),
        email_verified=bool(args.email_verified),
        uid=args.uid,
        update_existing=bool(args.update_existing),
        roles_only=bool(args.roles_only),
    )


def _resolve_user_for_roles_only(user_input: UserInput) -> auth.UserRecord:
    if user_input.uid:
        return auth.get_user(user_input.uid)
    return auth.get_user_by_email(user_input.email)


def main() -> None:
    user_input = _parse_args()
    _ensure_firebase_app()
    if user_input.roles_only:
        user = _resolve_user_for_roles_only(user_input)
    else:
        user = _create_or_update_auth_user(user_input)
    _write_role_record(user, user_input)
    print(f"User ready: {user.uid} ({user.email})")


if __name__ == "__main__":
    main()
