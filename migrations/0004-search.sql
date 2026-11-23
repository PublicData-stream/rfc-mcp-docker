CREATE INDEX relations_inbound_obsolescence
    ON rfc.rfc_relations (generation_id, target_rfc)
    WHERE kind = 'obsoletes';
