from types import SimpleNamespace

import scripts.tg_ingest as T


def _folder(title, include):
    return SimpleNamespace(title=title, include_peers=include, pinned_peers=[], exclude_peers=[])


def test_resolve_group_map_maps_channel_to_folder_group():
    folders = [
        _folder("official", [SimpleNamespace(id=1)]),
        _folder("opinion", [SimpleNamespace(id=2)]),
        _folder("Random", [SimpleNamespace(id=3)]),  # not a group folder → ignored
    ]
    gm = T.resolve_group_map(folders, peer_id=lambda p: p.id)
    assert gm == {1: "official", 2: "opinion"}


def test_resolve_group_map_conflict_takes_priority(caplog):
    # channel 9 is in both opinion and official → priority order: opinion wins
    folders = [
        _folder("official", [SimpleNamespace(id=9)]),
        _folder("opinion", [SimpleNamespace(id=9)]),
    ]
    gm = T.resolve_group_map(folders, peer_id=lambda p: p.id)
    assert gm[9] == "opinion"
