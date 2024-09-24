import pytest
from pvconsumer.pv_systems import get_pv_systems


@pytest.skip("This test uses pvoutput.org which we not longer use")
def test_get_pv_systems(db_session, filename):
    pv_systems = get_pv_systems(session=db_session, filename=filename, provider="pvoutput.org")

    assert len(pv_systems)
