-- A final outcome, once committed, can be neither changed nor removed.
CREATE FUNCTION guard_final() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status = 'FINAL' AND (TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD) THEN
    RAISE EXCEPTION 'immutable final %', OLD.transaction_id USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER immutable_final BEFORE UPDATE OR DELETE ON authorizations
  FOR EACH ROW EXECUTE FUNCTION guard_final();
