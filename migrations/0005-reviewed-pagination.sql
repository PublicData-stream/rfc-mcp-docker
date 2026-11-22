ALTER TABLE rfc.document_versions DROP CONSTRAINT document_versions_notice_policy_check;
ALTER TABLE rfc.document_versions ADD CONSTRAINT document_versions_notice_policy_check
    CHECK (notice_policy IN ('unresolved', 'metadata_only', 'whole_only', 'excerpt_allowed', 'whole_paginated'));
