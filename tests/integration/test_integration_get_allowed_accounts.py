import asyncio
import pytest

from podping_hivewriter import config, run
from podping_hivewriter.podping_hivewriter import PodpingHivewriter
from podping_hivewriter.constants import SERVER_ACCOUNT_NOT_AUTHORISED_TO_PODPING


@pytest.mark.asyncio
async def test_get_allowed_accounts():
    # Checks the allowed accounts checkup
    podping_hivewriter = PodpingHivewriter(
        config.Config.server_account,
        config.Config.posting_key,
        config.Config.nodes_in_use,
        operation_id="podping-livetest",
        resource_test=False,
        daemon=False,
        use_testnet=config.Config.test,
    )
    test_result = await podping_hivewriter.hive_wrapper.recheck_allowed_accounts()
    assert test_result

    podping_hivewriter.hive_wrapper.server_account = "badsetting"
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        await podping_hivewriter.hive_wrapper.recheck_allowed_accounts()

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == SERVER_ACCOUNT_NOT_AUTHORISED_TO_PODPING
