"""We noticed that for lots of the passive sites, the tilt was wrong in the database."""

import os

from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.read.site import get_all_sites

from pvconsumer.solar_sheffield_passiv import get_all_systems_from_solar_sheffield

url = os.getenv("SS_URL")
user_id = os.getenv("SS_USER_ID")
key = os.getenv("SS_KEY")
ocf_url_db = os.getenv("URL_DB")

all_pv_systems_df = get_all_systems_from_solar_sheffield()

connection = DatabaseConnection(url=url, echo=True)
with connection.get_session() as session:

    # get all sites
    sites = get_all_sites(session)
    sites_found = 0

    for site in sites:
        all_pv_systems_df_temp = all_pv_systems_df[
            all_pv_systems_df["pv_system_id"] == site.client_site_id
        ]

        if len(all_pv_systems_df_temp) == 0:
            print(f"Site {site.client_site_id} not found in solar sheffield")
            continue
        else:
            sites_found += 1
            print(f"Site {site.client_site_id} found in solar sheffield")
            old_tilt = site.tilt
            new_tilt = all_pv_systems_df_temp.iloc[0].tilt
            print(f"Old tilt: {old_tilt}, new tilt: {new_tilt}")
            if new_tilt != "None":
                new_tilt = float(new_tilt)

            if (new_tilt != "None") and (new_tilt is not None) and (old_tilt != new_tilt):
                site.tilt = new_tilt
                print(f"Updated tilt for site {site.client_site_id} from {old_tilt} to {new_tilt}")
                session.commit()

    print(f"Found {sites_found} sites in solar sheffield")
