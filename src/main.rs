use env_logger::{Builder, Env};
use log::{error, info};
// mod schema;
use cosmogony::{Cosmogony, Zone, ZoneType};
use failure::Error;
use geo_types::{MultiPolygon, Point};
use itertools::Itertools;
use std::iter::Iterator;
use structopt;
use structopt::StructOpt;
use wkt::ToWkt;
use postgres::{Connection, TlsMode};

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
    id: usize,
    name: String,
    uri: String,
    post_code: Option<String>,
    insee: Option<String>,
    level: Option<u32>,
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
        let zip_code = zone
            .tags
            .get("addr:postcode")
            .or_else(|| zone.tags.get("postal_code"))
            .map_or("", |val| &val[..])
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .sorted();

        let post_code = zip_code.into_iter().next();
        Self {
            id: zone.id.index,
            name: zone.name,
            uri: uri,
            insee: insee,
            level: Some(8), // Note: we hardcode the 8 level because 'ed' consider that a city is level 8
            post_code: post_code,
            coord: zone.center,
            boundary: zone.boundary,
        }
    }
}

fn default() -> String {
    "DEFAULT".to_owned()
}

impl AdministrativeRegion {
    fn to_sql(self) -> String {
        let coord = self
            .coord
            .map(|c| c.into())
            .map(|g: geo_types::Geometry<_>| g.to_wkt())
            .map(|w| w.items[0].to_string())
            .unwrap_or_else(default);
        let boundary = self
            .boundary
            .map(|c| c.into())
            .map(|g: geo_types::Geometry<_>| g.to_wkt())
            .map(|w| w.items[0].to_string())
            .unwrap_or_else(default);

        format!("INSERT INTO {table} VALUES ({id}, \'{name}\', \'{uri}\', \'{post_code}\', \'{insee}\', \'{level}\', \'{coord}\', \'{boundary}\')", 
            table = "administrative_regions",
            name=self.name, id=self.id, uri=self.uri,
            post_code = self.post_code.unwrap_or_else(default), 
            insee = self.insee.unwrap_or_else(default),
            level = self.level.map(|l|l.to_string()).unwrap_or_else(default),
            coord = coord,
            boundary = boundary
        )
    }
}

fn load_cosmogony(input: &str) -> Result<Cosmogony, Error> {
    serde_json::from_reader(std::fs::File::open(&input)?)
        .map_err(|e| failure::err_msg(e.to_string()))
}

fn send_to_pg(admins: impl Iterator<Item = String>, connection: Connection) -> Result<(), Error> {
    admins.for_each(|a| {
        info!("{}", a);
        connection.execute(&a, &[]).unwrap();

    });
    unimplemented!()
}

fn index_cities(args: Args) -> Result<(), Error> {
    info!("importing cosmogony into cities");
    let cosmogony = load_cosmogony(&args.input)?;

    let cities = cosmogony
        .zones
        .into_iter()
        .filter(|z| z.zone_type == Some(ZoneType::City))
        .map(|z| z.into())
        .map(|a: AdministrativeRegion| a.to_sql());

    let cnx = Connection::connect(args.connection_string, TlsMode::None)
        .expect(&format!("Error connecting to db"));
    send_to_pg(cities, cnx)?;

    Ok(())
}

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("Hello, world!");
    if let Err(err) = index_cities(Args::from_args()) {
        for cause in err.iter_chain() {
            error!("{}", cause);
        }
        std::process::exit(1)
    }
}
