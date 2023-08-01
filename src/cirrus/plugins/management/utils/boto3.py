import os

import boto3
import botocore.session
from botocore import credentials

from cirrus.plugins.management.exceptions import SSOError

# From https://github.com/boto/botocore/pull/1157#issuecomment-387580482


def get_mfa_session(**kwargs):
    """Get a session that supports caching the MFA session token.

    Returns a boto3 session object.

    Supports all kwargs of botocore.session. Of interest:

        profile: The name of the profile to use for this
            session.  Note that the profile can only be set when
            the session is created.

        session_vars: A dictionary that is used to override some or all
            of the environment variables associated with this session.  The
            key/value pairs defined in this dictionary will override the
            corresponding variables defined in ``SESSION_VARIABLES``.
    """
    profile = kwargs.get("profile", None)

    # Construct botocore session with cache
    session = botocore.session.Session(**kwargs)
    provider = session.get_component("credential_provider").get_provider("assume-role")
    if profile:
        # If ``profile`` is provided, then we need to
        # change the cache path from the default of
        # ~/.aws/boto/cache to the one used by awscli.
        # Without ``profile``, we defer to normal boto operations.
        working_dir = os.path.join(os.path.expanduser("~"), ".aws/cli/cache")
        provider.cache = credentials.JSONFileCache(working_dir)

    return boto3.Session(botocore_session=session)


def validate_session(session, profile):
    try:
        session.client("sts").get_caller_identity()
    except boto3.exceptions.botocore.exceptions.UnauthorizedSSOTokenError:
        raise SSOError(
            f"SSO session not authorized. Run `aws sso login --profile {profile}` and try again.",
        ) from None

    return session
