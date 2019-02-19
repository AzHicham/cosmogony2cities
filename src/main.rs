use cosmogony::{Cosmogony, Zone, ZoneType};
use env_logger::{Builder, Env};
use failure::Error;
use geo_types::{MultiPolygon, Point};
use itertools::Itertools;
use log::{error, info};
use postgres::{types::ToSql, Connection, TlsMode};
use std::iter::Iterator;
use structopt;
use structopt::StructOpt;
use wkt::ToWkt;

#[derive(StructOpt, Debug)]
#[structopt(name = "cosmogony2cities")]
struct Args {
    /// cosmogony file
    #[structopt(short = "i", long = "input")]
    input: String,

    #[structopt(
        short = "c",
        long = "connection-string",
        default_value = "postgres://postgres:postgres@localhost/cities"
    )]
    connection_string: String,
}

pub struct AdministrativeRegion {
    id: i32,
    name: String,
    uri: String,
    post_code: Option<String>,
    insee: Option<String>,
    level: Option<i32>,
    coord: Option<Point<f64>>,
    boundary: Option<MultiPolygon<f64>>,
}

impl From<Zone> for AdministrativeRegion {
    fn from(zone: Zone) -> Self {
        let insee = zone
            .tags
            .get("ref:INSEE")
            .map(|v| v.trim_left_matches('0').to_string());
        let uri = if let Some(insee) = &insee {
            format!("admin:osm:{}", insee)
        } else {
            format!("admin:osm:{}", zone.osm_id)
        };
        let mut zip_code = zone
            .tags
            .get("addr:postcode")
            .or_else(|| zone.tags.get("postal_code"))
            .map_or("", |val| &val[..])
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .sorted();

        let post_code = zip_code.next();
        Self {
            id: zone.id.index as i32,
            name: zone.name,
            uri,
            insee,
            level: Some(8), // Note: we hardcode the 8 level because 'ed' consider that a city is level 8
            post_code,
            coord: zone.center,
            boundary: zone.boundary,
        }
    }
}

impl AdministrativeRegion {
    fn into_sql_params(self) -> Vec<Box<dyn ToSql>> {
        let coord = self
            .coord
            .map(|c| c.into())
            .map(|g: geo_types::Geometry<_>| g.to_wkt())
            .map(|w| w.items[0].to_string());
        let boundary = self
            .boundary
            .map(|c| c.into())
            .map(|g: geo_types::Geometry<_>| g.to_wkt())
            .map(|w| w.items[0].to_string());

        vec![
            Box::new(self.id),
            Box::new(self.name),
            Box::new(self.uri),
            Box::new(self.post_code),
            Box::new(self.insee),
            Box::new(self.level),
            Box::new(coord),
            Box::new(boundary),
        ]
    }
}

fn load_cosmogony(input: &str) -> Result<Cosmogony, Error> {
    serde_json::from_reader(std::fs::File::open(&input)?)
        .map_err(|e| failure::err_msg(e.to_string()))
}

fn send_to_pg(
    admins: impl Iterator<Item = Vec<Box<ToSql>>>,
    connection: &Connection,
) -> Result<(), Error> {
    admins.for_each(|a| {
        let b = a.iter().map(|v| &**v).collect::<Vec<_>>();
        connection
            .execute(
                "INSERT INTO administrative_regions VALUES ($1, $2, $3, $4, $5, $6, ST_GeomFromText($7), ST_GeomFromText($8))",
                &b,
            )
            .unwrap();
    });
    Ok(())
}

fn import_zones(zones: impl IntoIterator<Item = Zone>, cnx: &Connection) -> Result<(), Error> {
    let cities = zones
        .into_iter()
        .filter(|z| z.zone_type == Some(ZoneType::City))
        .map(|z| z.into())
        .map(|a: AdministrativeRegion| a.into_sql_params());

    send_to_pg(cities, &cnx)
}

fn index_cities(args: Args) -> Result<(), Error> {
    info!("importing cosmogony into cities");
    let cnx =
        Connection::connect(args.connection_string, TlsMode::None).expect("Error connecting to db");

    let cosmogony = load_cosmogony(&args.input)?;

    import_zones(cosmogony.zones, &cnx)?;

    Ok(())
}

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    if let Err(err) = index_cities(Args::from_args()) {
        for cause in err.iter_chain() {
            error!("{}", cause);
        }
        std::process::exit(1)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use testcontainers::{clients, images, Docker};

    fn startup_db() -> Connection {
        let docker = clients::Cli::default();

        let db = "gis";
        let user = "gis";
        let password = "gis";

        let generic_postgres = images::generic::GenericImage::new("mdillon/postgis")
            .with_wait_for(images::generic::WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", db)
            .with_env_var("POSTGRES_USER", user)
            .with_env_var("POSTGRES_PASSWORD", password);

        let node = docker.run(generic_postgres);
        let cnx_string = format!(
            "postgres://{}:{}@localhost:{}/{}",
            user,
            password,
            node.get_host_port(5432).unwrap(),
            db
        );

        // let conn = wait_for_startup(&cnx_string).unwrap();
        let conn = Connection::connect(cnx_string, TlsMode::None).expect("unable to connect to db");

        conn.execute(
            r#"CREATE TABLE administrative_regions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    uri TEXT NOT NULL,
    post_code TEXT,
    insee TEXT,
    level integer,
    coord geography(Point,4326),
    boundary geography(MultiPolygon,4326)
);"#,
            &[],
        )
        .unwrap();

        conn
            .execute(
                "CREATE INDEX administrative_regions_boundary_idx ON administrative_regions USING gist (boundary);",
                &[],
            )
            .unwrap();

        conn
    }

    struct TestInitializer<'a> {
        pub conn: &'a Connection,
    }
    impl<'a> TestInitializer<'a> {
        fn new(conn: &'a Connection) -> Self {
            conn.execute("TRUNCATE TABLE administrative_regions;", &[])
                .unwrap();
            TestInitializer { conn }
        }
    }

    #[test]
    fn tests() {
        let cnx = startup_db();
        test_null_boundaries(TestInitializer::new(&cnx));
        test_boundaries_and_insee(TestInitializer::new(&cnx));
    }

    fn test_null_boundaries(test_init: TestInitializer) {
        let cnx = test_init.conn;

        let mut zone = cosmogony::Zone::default();
        zone.id = cosmogony::ZoneIndex { index: 0 };
        zone.name = "toto".to_owned();
        zone.osm_id = "bob".to_owned();
        zone.zone_type = Some(cosmogony::ZoneType::City);
        import_zones(std::iter::once(zone), &cnx).unwrap();

        let rows = cnx
            .query("SELECT id, name, uri, level, post_code, insee,
            ST_ASTEXT(coord) as coord, ST_ASTEXT(boundary) as boundary FROM administrative_regions;", &[])
            .unwrap();

        assert_eq!(rows.len(), 1);
        let r = rows.get(0);
        assert_eq!(r.get::<_, String>("name"), "toto".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:osm:bob".to_owned());
        assert_eq!(r.get::<_, i32>("id"), 0);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, Option<String>>("post_code"), None);
        assert_eq!(r.get::<_, Option<String>>("insee"), None);
        assert_eq!(r.get::<_, Option<String>>("coord"), None);
        assert_eq!(r.get::<_, Option<String>>("boundary"), None);
    }

    fn test_boundaries_and_insee(test_init: TestInitializer) {
        let cnx = test_init.conn;

        let mut zone = cosmogony::Zone::default();
        zone.id = cosmogony::ZoneIndex { index: 1 };
        zone.name = "toto".to_owned();
        zone.tags = vec![("ref:INSEE", "75111"), ("addr:postcode", "75011")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        zone.zone_type = Some(cosmogony::ZoneType::City);
        zone.center = Some((12., 14.).into());
        let poly = geo_types::Polygon::new(
            (vec![(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]).into(),
            Vec::new(),
        );
        let multipoly = MultiPolygon(vec![poly]);
        zone.boundary = Some(multipoly);

        import_zones(std::iter::once(zone), &cnx).unwrap();

        let rows = cnx
            .query("SELECT id, name, uri, level, post_code, insee,
            ST_ASTEXT(coord) as coord, ST_ASTEXT(boundary) as boundary FROM administrative_regions;", &[])
            .unwrap();

        assert_eq!(rows.len(), 1);
        let r = rows.get(0);
        assert_eq!(r.get::<_, String>("name"), "toto".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:osm:75111".to_owned());
        assert_eq!(r.get::<_, i32>("id"), 1);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, String>("post_code"), "75011".to_owned());
        assert_eq!(r.get::<_, String>("insee"), "75111".to_owned());
        assert_eq!(r.get::<_, String>("coord"), "POINT(12 14)".to_owned());
        assert_eq!(
            r.get::<_, String>("boundary"),
            "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))".to_owned()
        );
    }

}
