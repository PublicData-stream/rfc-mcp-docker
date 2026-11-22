import json
import subprocess
import unittest
from unittest.mock import patch

from scripts import ghcr_image


class ScheduleTest(unittest.TestCase):
    def test_alternate_weeks_continue_across_new_year(self):
        self.assertEqual(ghcr_image.schedule("schedule", "2026-12-20T03:17:00Z"), (True, "20261220"))
        self.assertEqual(ghcr_image.schedule("schedule", "2026-12-27T03:17:00Z"), (False, "20261227"))
        self.assertEqual(ghcr_image.schedule("schedule", "2027-01-03T03:17:00Z"), (True, "20270103"))

    def test_manual_dispatch_bypasses_week_gate_and_uses_utc_date(self):
        self.assertEqual(
            ghcr_image.schedule("workflow_dispatch", "2026-12-26T23:59:59-02:00"),
            (True, "20261227"),
        )


class ReleaseTest(unittest.TestCase):
    parents = {
        "pin": ["left", "right"],
        "left": ["base"],
        "right": ["base"],
        "base": [],
    }

    def test_pinned_release_takes_precedence(self):
        names = {"pin": ["1.0.0"], "left": ["1.2.0"], "right": ["1.10.0"]}
        self.assertEqual(ghcr_image.nearest_release("pin", self.parents, names), "1.0.0")

    def test_nearest_release_prefers_highest_numeric_version_on_tie(self):
        names = {"left": ["1.2.0"], "right": ["1.10.0"], "base": ["2.0.0"], "other": ["3.0.0"]}
        self.assertEqual(ghcr_image.nearest_release("pin", self.parents, names), "1.10.0")

    def test_ineligible_release_titles_do_not_supply_version(self):
        release = {
            "name": "0.6.1", "tag_name": "v0.6.1", "published_at": "2026-10-30",
            "prerelease": False, "draft": False,
        }
        self.assertTrue(ghcr_image.stable_release(release))
        self.assertFalse(ghcr_image.stable_release({**release, "prerelease": True}))
        self.assertFalse(ghcr_image.stable_release({**release, "draft": True}))
        self.assertFalse(ghcr_image.stable_release({**release, "tag_name": "index-2026-10-24"}))
        self.assertFalse(ghcr_image.stable_release({**release, "name": "RFC index 2026-10-24"}))

    def test_missing_ancestor_reports_pin(self):
        with self.assertRaisesRegex(ValueError, "ancestor of upstream pin"):
            ghcr_image.nearest_release("pin", self.parents, {"other": ["3.0.0"]})


class ImageDigestTest(unittest.TestCase):
    image = "ghcr.io/publicdata-stream/rfc-mcp-docker"
    tag = "0.6.1-build.20261122"
    index = "sha256:" + "a" * 64
    tested = {"amd64": "sha256:" + "b" * 64, "arm64": "sha256:" + "c" * 64}
    labels = {
        ghcr_image.WRAPPER_REVISION: "wrapper",
        ghcr_image.UPSTREAM_REVISION: "pin",
    }

    def inspect(self, reference, field):
        if field == "Manifest":
            payload = {
                "digest": self.index,
                "manifests": [
                    {"platform": {"os": "linux", "architecture": arch}, "digest": digest}
                    for arch, digest in self.tested.items()
                ],
            }
        else:
            payload = {"config": {"Labels": self.labels}}
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    def digest(self, **kwargs):
        return ghcr_image.image_digest(self.image, self.tag, "wrapper", "pin", **kwargs)

    def test_reuses_same_source_and_checks_expected_platform_digests(self):
        with patch.object(ghcr_image, "imagetools_inspect", side_effect=self.inspect):
            self.assertEqual(self.digest(), self.index)
            self.assertEqual(self.digest(expected=self.tested), self.index)
            with self.assertRaisesRegex(ValueError, "tested platform digests"):
                self.digest(expected={"amd64": self.tested["arm64"], "arm64": self.tested["amd64"]})

    def test_rejects_same_day_tag_from_a_different_revision(self):
        self.labels = {**self.labels, ghcr_image.UPSTREAM_REVISION: "different"}
        with patch.object(ghcr_image, "imagetools_inspect", side_effect=self.inspect):
            with self.assertRaisesRegex(ValueError, "different.*source revision"):
                self.digest()

    def test_distinguishes_missing_tag_from_registry_auth_error(self):
        missing = subprocess.CompletedProcess([], 1, "", f"ERROR: {self.image}:{self.tag}: not found\n")
        denied = subprocess.CompletedProcess(
            [], 1, "", "ERROR: failed to fetch token from https://ghcr.io/token?scope=test: 403 Forbidden"
        )
        with patch.object(ghcr_image, "imagetools_inspect", return_value=missing):
            self.assertEqual(self.digest(), "")
        with patch.object(ghcr_image, "imagetools_inspect", return_value=denied):
            self.assertEqual(self.digest(allow_uninitialized=True), "")
            with self.assertRaisesRegex(ValueError, "403 Forbidden"):
                self.digest()
