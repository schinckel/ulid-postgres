CREATE OR REPLACE FUNCTION valid_ulid(TEXT)
RETURNS BOOLEAN AS $$
  SELECT char_length($1) = 26 AND trim('0123456789ABCDEFGHJKMNPQRSTVWXYZ' from upper($1)) = '';
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION encode_char(INTEGER)
RETURNS char(1) AS $$
  SELECT substring('0123456789ABCDEFGHJKMNPQRSTVWXYZ'::TEXT FROM ($1 + 1) FOR 1);
$$ LANGUAGE SQL IMUTABLE;

CREATE DOMAIN ulid AS char(26) CHECK (valid_ulid(VALUE));


CREATE OR REPLACE FUNCTION encode_for_ulid(value BIGINT, bytes INTEGER)
RETURNS TEXT AS $$
WITH RECURSIVE encode(remaining, ulid) AS (
    SELECT $1, ''::TEXT

    UNION

    SELECT remaining / 32, encode_char((remaining % 32)::INTEGER) || ulid
    FROM encode
    WHERE char_length(ulid) < $2
  )
  SELECT ulid FROM encode ORDER BY char_length(ulid) DESC LIMIT 1;
  $$ LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION generate_ulid(epoch BIGINT)
RETURNS ulid AS $$
  SELECT (encode_for_ulid(epoch, 10) || encode_for_ulid((random() * 10^16)::BIGINT, 16))::ULID;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION generate_ulid() RETURNS ulid AS $$
  SELECT generate_ulid((EXTRACT(epoch FROM clock_timestamp()) * 1000)::BIGINT);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION decode_chars(TEXT) RETURNS BIGINT AS $$
  WITH RECURSIVE decode(value, index) AS(
    SELECT 0::BIGINT, 0

    UNION ALL

    SELECT value + (32^(char_length($1) - index - 1))::BIGINT * (
            position(substring($1 FROM index + 1 FOR 1) IN '0123456789ABCDEFGHJKMNPQRSTVWXYZ'::TEXT) - 1),
           index + 1
      FROM decode
     WHERE index <= char_length($1) + 1
  )
  SELECT value::BIGINT FROM decode ORDER BY index DESC LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION seconds(ulid) RETURNS INTEGER AS $$
  SELECT (decode_chars(substring($1::TEXT FROM 1 FOR 10)) / 1000)::INTEGER;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION to_tstz(ulid) RETURNS TIMESTAMPTZ AS $$
  SELECT '1970-01-01 00:00:00'::TIMESTAMPTZ + (seconds($1) * INTERVAL '1 second');
$$ LANGUAGE SQL IMMUTABLE;


-- This one doesn't work yet, see below.
CREATE OR REPLACE FUNCTION sharding(ulid, partitions INTEGER) RETURNS BIGINT AS $$
  SELECT decode_chars(substring($1 FROM 10 FOR 16)) % partitions;
$$ LANGUAGE SQL IMMUTABLE;


-- Postgres BIGINT is not wide enough for this.
-- NUMERIC seems to give incorrect values, and it's too late to think more.
WITH RECURSIVE decode(value, index, current, power, position) AS(
  SELECT 0::NUMERIC,
         0,
         NULL::TEXT,
         NULL::NUMERIC,
         NULL::INTEGER

  UNION ALL

  SELECT value + (32^(16 - index - 1))::NUMERIC * (position(substring('0000053TPJSYAP1D' FROM index + 1 FOR 1) IN '0123456789ABCDEFGHJKMNPQRSTVWXYZ'::TEXT) - 1),
         index + 1,
         substring('0000053TPJSYAP1D' FROM index + 1 FOR 1),
         (32^(16 - index - 1))::NUMERIC,
         position(substring('0000053TPJSYAP1D' FROM index + 1 FOR 1) IN '0123456789ABCDEFGHJKMNPQRSTVWXYZ'::TEXT) - 1
    FROM decode
   WHERE index <= char_length('0000053TPJSYAP1D')
)
SELECT * FROM decode ORDER BY index LIMIT 100;
