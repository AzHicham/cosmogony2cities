use cosmogony::{Cosmogony, Zone, ZoneType};
use env_logger::{Builder, Env};
use failure::Error;
use geo_types::{MultiPolygon, Point};
use itertools::Itertools;
use log::{error, info};
use postgres::types::ToSql;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};
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

fn load_cosmogony(input: &str) -> Result<Cosmogony, Error> {
    serde_json::from_reader(std::fs::File::open(&input)?)
        .map_err(|e| failure::err_msg(e.to_string()))
}

fn send_to_pg(
    admins: impl Iterator<Item = Vec<Box<ToSql + Send + Sync>>>,
    cnx_pool: &r2d2::Pool<PostgresConnectionManager>,
) -> Result<(), Error> {
    use par_map::ParMap;
    let pool = cnx_pool.clone();
    admins
        .pack(100)
        .par_map(move |admins_chunks| {
            let mut query = "INSERT INTO administrative_regions VALUES ".to_owned();

            let nb_admins = admins_chunks.len();
            log::info!("bulk inserting {} admins", nb_admins);

            let params = admins_chunks
                .iter()
                .flat_map(|a| a.iter().map(|v| &**v as &dyn postgres::types::ToSql))
                .collect::<Vec<&dyn postgres::types::ToSql>>();

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

            log::debug!(
                "elt: {} -- params: {} -- query: {}",
                nb_admins,
                params.len(),
                &query
            );
            let connection = pool.get().unwrap();
            if let Err(e) = connection.execute(&query, params.as_slice()) {
                log::warn!("invalid query: {}, error: {}", query, e); //TODO return an error
            }
        })
        .for_each(|_| {});

    // admins.for_each(move |a| {
    //     let b = a.iter().map(|v| &**v as &dyn postgres::types::ToSql).collect::<Vec<_>>();
    //         let connection = pool.get().unwrap();
    //     connection
    //         .execute(
    //             "INSERT INTO administrative_regions VALUES ($1, $2, $3, $4, $5, $6, ST_GeomFromText($7), ST_GeomFromText($8))",
    //             &b,
    //         ).unwrap();
    // });

    Ok(())
}

fn import_zones(
    zones: impl IntoIterator<Item = Zone>,
    cnx_pool: &r2d2::Pool<PostgresConnectionManager>,
) -> Result<(), Error> {
    let cities = zones
        .into_iter()
        .filter(|z| z.zone_type == Some(ZoneType::City))
        .map(|z| z.into())
        .map(|a: AdministrativeRegion| a.into_sql_params());

    send_to_pg(cities, cnx_pool)
}

fn index_cities(args: Args) -> Result<(), Error> {
    info!("importing cosmogony into cities");

    let manager = PostgresConnectionManager::new(args.connection_string, TlsMode::None)
        .expect("Error connecting to db");
    let pool = r2d2::Pool::new(manager).expect("impossible to create pool");

    let cosmogony = load_cosmogony(&args.input)?;

    import_zones(cosmogony.zones, &pool)?;

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

    fn startup_db() -> r2d2::Pool<r2d2_postgres::PostgresConnectionManager> {
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

        let manager = PostgresConnectionManager::new(cnx_string, r2d2_postgres::TlsMode::None)
            .expect("Error connecting to db");
        let pool = r2d2::Pool::new(manager).expect("impossible to create pool");

        let conn = pool.get().expect("unable to connect to db");

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

        pool
    }

    #[test]
    fn tests() {
        let pool = startup_db();

        let cnx = pool.get().unwrap();

        let mut zone1 = cosmogony::Zone::default();
        zone1.id = cosmogony::ZoneIndex { index: 0 };
        zone1.name = "toto".to_owned();
        zone1.osm_id = "bob".to_owned();
        zone1.zone_type = Some(cosmogony::ZoneType::City);

        let mut zone2 = cosmogony::Zone::default();
        zone2.id = cosmogony::ZoneIndex { index: 1 };
        zone2.name = "toto".to_owned();
        zone2.tags = vec![("ref:INSEE", "75111"), ("addr:postcode", "75011")]
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

        let zones = vec![zone1, zone2];
        import_zones(zones, &pool).unwrap();

        let rows = cnx
            .query("SELECT id, name, uri, level, post_code, insee,
            ST_ASTEXT(coord) as coord, ST_ASTEXT(boundary) as boundary FROM administrative_regions;", &[])
            .unwrap();

        assert_eq!(rows.len(), 2);
        let r = rows.get(0);
        assert_eq!(r.get::<_, String>("name"), "toto".to_owned());
        assert_eq!(r.get::<_, String>("uri"), "admin:osm:bob".to_owned());
        assert_eq!(r.get::<_, i32>("id"), 0);
        assert_eq!(r.get::<_, i32>("level"), 8);
        assert_eq!(r.get::<_, Option<String>>("post_code"), None);
        assert_eq!(r.get::<_, Option<String>>("insee"), None);
        assert_eq!(r.get::<_, Option<String>>("coord"), None);
        assert_eq!(r.get::<_, Option<String>>("boundary"), None);

        let r = rows.get(1);
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
