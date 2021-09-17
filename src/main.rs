use cosmogony::{Zone, ZoneType};
use env_logger::{Builder, Env};
use failure::Error;
use geo_types::{MultiPolygon, Point};
use itertools::Itertools;
use log::{error, info};
use postgres::{types::ToSql, Connection, TlsMode};
use std::iter::Iterator;
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
    id: i64,
    name: String,
    uri: String,
    post_code: Option<String>,
    insee: Option<String>,
    level: Option<i32>,
    coord: Option<Point<f64>>,
    boundary: Option<MultiPolygon<f64>>,
}

fn format_zip_codes(zip_codes: &[String]) -> Option<String> {
    match zip_codes.len() {
        0 => None,
        1 => Some(zip_codes.first().unwrap().to_string()),
        _ => Some(format!(
            "{}-{}",
            zip_codes.first().unwrap(),
            zip_codes.last().unwrap()
        )),
    }
}

impl From<Zone> for AdministrativeRegion {
    fn from(zone: Zone) -> Self {
        let insee = zone
            .tags
            .get("ref:INSEE")
            .map(|v| v.to_string());
        let uri = if let Some(insee) = &insee {
            format!("admin:fr:{}", insee)
        } else {
            format!("admin:osm:{}", zone.osm_id)
        };
        let zip_codes: Vec<_> = zone
            .tags
            .get("addr:postcode")
            .or_else(|| zone.tags.get("postal_code"))
            .map_or("", |val| &val[..])
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .sorted()
            .collect();

        let post_code = format_zip_codes(&zip_codes);
        Self {
            id: zone.id.index as i64,
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
    fn into_sql_params(self) -> Vec<Box<dyn ToSql + Send + Sync>> {
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

fn send_to_pg(
    admins: impl Iterator<Item = Vec<Box<dyn ToSql + Send + Sync>>>,
    cnx: &Connection,
) -> Result<(), Error> {
    use par_map::ParMap;

    let transaction = cnx.transaction()?;
    transaction.execute("TRUNCATE TABLE administrative_regions;", &[])?;

    for (query, admins_chunks) in admins.pack(500).par_map(move |admins_chunks| {
        let mut query = "INSERT INTO administrative_regions VALUES ".to_owned();

        let nb_admins = admins_chunks.len();

        for i in 0..nb_admins {
            let base_cpt = i * 8;
            if i != 0 {
                query += ", ";
            }
            query += &format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ST_GeomFromText(${}), ST_GeomFromText(${}))",
                base_cpt + 1,
                base_cpt + 2,
                base_cpt + 3,
                base_cpt + 4,
                base_cpt + 5,
                base_cpt + 6,
                base_cpt + 7,
                base_cpt + 8,
            );
        }
        query += ";";
        (query, admins_chunks)
    }) {
        log::info!("bulk inserting {} admins", admins_chunks.len());
        let params = admins_chunks
            .iter()
            .flat_map(|a| a.iter().map(|v| &**v as &dyn postgres::types::ToSql))
            .collect::<Vec<&dyn postgres::types::ToSql>>();

        log::debug!("query: {} -- params {:?}", &query, &params);

        transaction.execute(&query, params.as_slice())?;
    }

    transaction.commit()?;
    Ok(())
}

fn import_zones(zones: impl IntoIterator<Item = Zone>, cnx: &Connection) -> Result<(), Error> {
    let cities = zones
        .into_iter()
        .filter(|z| z.zone_type == Some(ZoneType::City))
        .map(|z| z.into())
        .map(|a: AdministrativeRegion| a.into_sql_params());

    send_to_pg(cities, cnx)
}

fn index_cities(args: Args) -> Result<(), Error> {
    info!("importing cosmogony into cities");

    let cnx =
        Connection::connect(args.connection_string, TlsMode::None).expect("Error connecting to db");

    let zones = cosmogony::read_zones_from_file(&args.input)?.filter_map(|r| {
        r.map_err(|e| log::warn!("impossible to read zone: {}", e))
            .ok()
    });

    info!("cosmogony loaded, importing it in db");
    import_zones(zones, &cnx)?;

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

    #[test]
    fn tests() {
        Builder::from_env(Env::default().default_filter_or("info")).init();
        info!("starting up the test database");
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

        info!("running the docker");
        let node = docker.run(generic_postgres);
        info!("docker started");
        let cnx_string = format!(
            "postgres://{}:{}@localhost:{}/{}",
            user,
            password,
            node.get_host_port(5432).unwrap(),
            db
        );

        let conn = Connection::connect(cnx_string, TlsMode::None).expect("Error connecting to db");

        info!("preparing the db schema");
        conn.execute(
            r#"CREATE TABLE administrative_regions (
    id BIGINT PRIMARY KEY,
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

        let mut zone1 = cosmogony::Zone::default();
        zone1.id = cosmogony::ZoneIndex { index: 0 };
        zone1.name = "toto".to_owned();
        zone1.osm_id = "bob".to_owned();
        zone1.zone_type = Some(cosmogony::ZoneType::City);

        let mut zone2 = cosmogony::Zone::default();
        zone2.id = cosmogony::ZoneIndex { index: 1 };
        zone2.name = "toto".to_owned();
        zone2.tags = vec![("ref:INSEE", "75111"), ("addr:postcode", "75011;75111")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        zone2.zone_type = Some(cosmogony::ZoneType::City);
        zone2.center = Some((12., 14.).into());
        let poly = geo_types::Polygon::new(
            (vec![(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]).into(),
            Vec::new(),
        );
        let multipoly = MultiPolygon(vec![poly]);
        zone2.boundary = Some(multipoly);

        let mut zone3 = cosmogony::Zone::default();
        zone3.id = cosmogony::ZoneIndex { index: 2 };
        zone3.name = "insee with zero".to_owned();
        zone3.osm_id = "insee_with_zero".to_owned();
        zone3.tags = vec![("ref:INSEE", "01249"), ("addr:postcode", "01700")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        zone3.zone_type = Some(cosmogony::ZoneType::City);

        let zones = vec![zone1, zone2, zone3];
        import_zones(zones, &conn).unwrap();

        let rows = conn
            .query("SELECT id, name, uri, level, post_code, insee,
            ST_ASTEXT(coord) as coord, ST_ASTEXT(boundary) as boundary FROM administrative_regions;", &[])
            .expect("impossible to query db");

        assert_eq!(rows.len(), 3);
        let r = rows.get(0);
        assert_eq!(r.get::<_, String>("name"), "toto".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:osm:bob".to_owned());
        assert_eq!(r.get::<_, i64>("id"), 0);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, Option<String>>("post_code"), None);
        assert_eq!(r.get::<_, Option<String>>("insee"), None);
        assert_eq!(r.get::<_, Option<String>>("coord"), None);
        assert_eq!(r.get::<_, Option<String>>("boundary"), None);

        let r = rows.get(1);
        assert_eq!(r.get::<_, String>("name"), "toto".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:fr:75111".to_owned());
        assert_eq!(r.get::<_, i64>("id"), 1);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, String>("post_code"), "75011-75111".to_owned());
        assert_eq!(r.get::<_, String>("insee"), "75111".to_owned());
        assert_eq!(r.get::<_, String>("coord"), "POINT(12 14)".to_owned());
        assert_eq!(
            r.get::<_, String>("boundary"),
            "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))".to_owned()
        );

        let r = rows.get(2);
        assert_eq!(r.get::<_, String>("name"), "insee with zero".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:fr:01249".to_owned());
        assert_eq!(r.get::<_, i64>("id"), 2);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, String>("post_code"), "01700".to_owned());
        assert_eq!(r.get::<_, String>("insee"), "01249".to_owned());
        assert_eq!(r.get::<_, Option<String>>("coord"), None);
        assert_eq!(r.get::<_, Option<String>>("boundary"), None);
    }
}
