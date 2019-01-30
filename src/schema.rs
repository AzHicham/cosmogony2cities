table! {
    administrative_regions (id) {
        id -> Int4,
        name -> Text,
        uri -> Text,
        post_code -> Nullable<Text>,
        insee -> Nullable<Text>,
        level -> Nullable<Int4>,
        coord -> Nullable<Geography>,
        boundary -> Nullable<Geography>,
    }
}

table! {
    spatial_ref_sys (srid) {
        srid -> Int4,
        auth_name -> Nullable<Varchar>,
        auth_srid -> Nullable<Int4>,
        srtext -> Nullable<Varchar>,
        proj4text -> Nullable<Varchar>,
    }
}

allow_tables_to_appear_in_same_query!(administrative_regions, spatial_ref_sys,);
