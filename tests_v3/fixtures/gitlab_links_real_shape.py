"""Fixture: the VERBATIM GitLab links-endpoint response captured live from
https://gitlab.bdaya-dev.com/api/v4/projects/invora%2Finvora-flutter/issues/465/links
on 2026-09-14 (task_c7b6a2d6c594a077 lane).

invora/invora-flutter#465 carries one `is_blocked_by` link to open issue
invora/invora-flutter#462. NOTE THE SHAPE — this is what killed the grouped
intake cycle on every 60s poll (shared/claude-plugins bug, PR#40 follow-up):

  GET /projects/:id/issues/:iid/links returns a JSON ARRAY of linked-issue
  objects. Each object carries a SCALAR `link_type` ("is_blocked_by" /
  "blocks" / "relates_to") plus `state`, `project_id`, `iid`,
  `issue_link_id`. It is NOT an object with "open"/"closed" keys and the
  items have NO `link_types` LIST — that was the mock's wrong shape
  (shared/claude-plugins#895).

Only the link-relevant fields are kept (the real payload also carries full
issue bodies); `description` is elided from the capture because it holds no
shape the guard reads.
"""

# issue_id -> response array (as the mock transport should serve it)
LINKS_RESPONSES = {
    "invora/invora-flutter#465": [
        {
            "id": 5843,
            "iid": 462,
            "project_id": 275,
            "title": ("Share dialog QR must encode the invoice's shareable "
                      "PDF URL \u2014 today it renders decimal byte digits "
                      "from the ZATCA node it should not read at all"),
            "state": "opened",
            "closed_at": None,
            "labels": ["area::invoicing", "priority::p1", "stack::flutter",
                       "status::needs-decision", "type::bug"],
            "author": {"id": 228, "username": "bdaya-agent"},
            "type": "ISSUE",
            "references": {"short": "#462", "relative": "#462",
                           "full": "invora/invora-flutter#462"},
            "issue_link_id": 394,
            "link_type": "is_blocked_by",
            "link_created_at": "2026-09-02T23:20:08.165Z",
            "link_updated_at": "2026-09-02T23:20:08.165Z",
        },
    ],
    # The blocker's own view of the same link (link_type flipped) — shape
    # evidence, and useful for "blocker side" tests.
    "invora/invora-flutter#462": [
        {
            "id": 5848,
            "iid": 465,
            "project_id": 275,
            "title": ("Share dialog renders the ZATCA QR from List<int>.join() "
                      "\u2014 the QR encodes decimal byte digits, not the "
                      "base64 TLV"),
            "state": "opened",
            "closed_at": None,
            "labels": ["area::invoicing", "stack::flutter", "type::bug"],
            "author": {"id": 228, "username": "bdaya-agent"},
            "type": "ISSUE",
            "references": {"short": "#465", "relative": "#465",
                           "full": "invora/invora-flutter#465"},
            "issue_link_id": 394,
            "link_type": "blocks",
            "link_created_at": "2026-09-02T23:20:08.165Z",
            "link_updated_at": "2026-09-02T23:20:08.165Z",
        },
    ],
    # Issues with no links at all -> empty ARRAY (not a missing key, not {}).
    "invora/invora-flutter#463": [],
    "invora/invora-flutter#464": [],
    "invora/invora-flutter#467": [],
    "invora/invora-flutter#468": [],
}
