-- init table
CREATE TABLE administrative_regions (
    id SERIAL PRIMARY KEY,
    name text NOT NULL,
    uri text NOT NULL,
    post_code text,
    insee text,
    level integer,
    coord geography(Point,4326),
    boundary geography(MultiPolygon,4326)
);

CREATE INDEX administrative_regions_boundary_idx ON administrative_regions USING gist (boundary);
