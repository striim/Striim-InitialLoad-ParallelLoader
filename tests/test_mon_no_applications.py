import main


def _mon(apps, status="Success"):
    return [
        {
            "command": "mon",
            "executionStatus": status,
            "output": {
                "striimApplications": apps,
                "striimClusterNodes": [
                    {"entityType": "SERVER", "name": "S1", "version": "5.0.6.0C"}
                ],
            },
            "responseCode": 200,
        }
    ]


def test_no_applications_message_is_a_valid_empty_answer():
    apps, _, _, valid = main.map_mon_json_response(_mon({"message": "No applications"}))
    assert valid is True
    assert apps == []


def test_empty_list_is_still_valid():
    apps, _, _, valid = main.map_mon_json_response(_mon([]))
    assert valid is True
    assert apps == []


def test_app_without_full_name_is_still_invalid():
    _, _, _, valid = main.map_mon_json_response(_mon([{"entityType": "APPLICATION"}]))
    assert valid is False


def test_other_dict_is_still_invalid():
    _, _, _, valid = main.map_mon_json_response(_mon({"unexpected": 1}))
    assert valid is False


def test_other_message_is_still_invalid():
    for msg in ("Permission denied", "Failed to collect application metrics"):
        _, _, _, valid = main.map_mon_json_response(_mon({"message": msg}))
        assert valid is False, msg


def test_no_applications_in_a_failed_response_is_invalid():
    _, _, _, valid = main.map_mon_json_response(_mon({"message": "No applications"}, status="Failure"))
    assert valid is False


def test_no_applications_with_extra_fields_is_invalid():
    apps = {"message": "No applications", "error": "partial"}
    _, _, _, valid = main.map_mon_json_response(_mon(apps))
    assert valid is False
