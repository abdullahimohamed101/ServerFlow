-- allowed_models is a list of model names: a NULL element would be unreadable by the gateway (it
-- would fail that tenant's keys, see auth.ErrBadRecord) and means nothing. Forbid it.
ALTER TABLE tenants
    ADD CONSTRAINT tenants_allowed_models_no_null_elements
    CHECK (allowed_models IS NULL OR array_position(allowed_models, NULL) IS NULL);
