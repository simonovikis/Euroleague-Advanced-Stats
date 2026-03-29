"""
auth.py — Supabase Authentication & RBAC
==========================================
Handles user authentication via Supabase Auth and role-based
access control for the Streamlit application.

Admin users (defined in ADMIN_EMAILS) get access to the
Database Sync Manager. Regular users see the analytics dashboard only.
"""

import streamlit as st
from supabase import create_client, Client

from streamlit_app.utils.secrets_manager import SUPABASE_URL, SUPABASE_KEY, ADMIN_EMAILS


def get_supabase_client() -> Client:
    if "supabase_client" not in st.session_state:
        url = SUPABASE_URL.strip()
        key = SUPABASE_KEY.strip()

        if not url or not key:
            st.error(
                "Supabase credentials not configured. "
                "Set SUPABASE_URL and SUPABASE_KEY in .env or st.secrets."
            )
            st.stop()

        st.session_state["supabase_client"] = create_client(url, key)
    return st.session_state["supabase_client"]


def _get_admin_emails() -> list:
    return ADMIN_EMAILS


def init_auth_state():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if "user_email" not in st.session_state:
        st.session_state["user_email"] = None
    if "is_admin" not in st.session_state:
        st.session_state["is_admin"] = False


def _check_admin(email: str) -> bool:
    return email.strip().lower() in _get_admin_emails()


def _handle_login(email: str, password: str):
    try:
        client = get_supabase_client()
        response = client.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        user = response.user
        if user:
            st.session_state["authenticated"] = True
            st.session_state["user_email"] = user.email
            st.session_state["is_admin"] = _check_admin(user.email)
            st.rerun()
        else:
            st.error("Login failed. Please check your credentials.")
    except Exception as e:
        msg = str(e)
        if "Invalid login credentials" in msg:
            st.error("Invalid email or password.")
        elif "Email not confirmed" in msg:
            st.warning("Please verify your email before logging in.")
        else:
            st.error(f"Login error: {msg}")


def _handle_signup(email: str, password: str):
    try:
        client = get_supabase_client()
        response = client.auth.sign_up({"email": email, "password": password})
        if response.user:
            if response.user.identities:
                st.success(
                    "Account created! Check your email for a confirmation link, then log in."
                )
            else:
                st.warning(
                    "An account with this email may already exist. Try logging in."
                )
        else:
            st.error("Sign up failed. Please try again.")
    except Exception as e:
        msg = str(e)
        if "already registered" in msg.lower():
            st.warning("This email is already registered. Please log in instead.")
        else:
            st.error(f"Sign up error: {msg}")


def handle_logout():
    try:
        client = get_supabase_client()
        client.auth.sign_out()
    except Exception:
        pass
    st.session_state["authenticated"] = False
    st.session_state["user_email"] = None
    st.session_state["is_admin"] = False
    st.rerun()


def render_auth_page():
    st.markdown(
        '<h1 style="text-align:center; color:#e4e4f0; margin-bottom:4px;">'
        "🏀 Euroleague Advanced Analytics</h1>"
        '<p style="text-align:center; color:#9ca3af; font-size:1.05rem; '
        'margin-bottom:2rem;">Sign in to access the dashboard</p>',
        unsafe_allow_html=True,
    )

    _left, col_form, _right = st.columns([1, 2, 1])
    with col_form:
        tab_login, tab_signup = st.tabs(["Login", "Sign Up"])

        with tab_login:
            with st.form("login_form"):
                email = st.text_input("Email", key="login_email")
                password = st.text_input("Password", type="password", key="login_pw")
                submitted = st.form_submit_button("Log In", width="stretch")
                if submitted:
                    if not email or not password:
                        st.error("Please enter both email and password.")
                    else:
                        _handle_login(email, password)

        with tab_signup:
            with st.form("signup_form"):
                email = st.text_input("Email", key="signup_email")
                password = st.text_input("Password", type="password", key="signup_pw")
                confirm = st.text_input(
                    "Confirm Password", type="password", key="signup_confirm"
                )
                submitted = st.form_submit_button("Sign Up", width="stretch")
                if submitted:
                    if not email or not password:
                        st.error("Please enter both email and password.")
                    elif password != confirm:
                        st.error("Passwords do not match.")
                    elif len(password) < 6:
                        st.error("Password must be at least 6 characters.")
                    else:
                        _handle_signup(email, password)


def render_user_sidebar():
    email = st.session_state.get("user_email", "")
    is_admin = st.session_state.get("is_admin", False)
    role_badge = (
        ' <span style="color:#f59e0b; font-size:0.75rem;">ADMIN</span>'
        if is_admin
        else ""
    )
    st.markdown(
        f'<p style="color:#e4e4f0; font-size:0.85rem;">'
        f"👤 {email}{role_badge}</p>",
        unsafe_allow_html=True,
    )
    if st.button("Logout", key="btn_logout", width="stretch"):
        handle_logout()
