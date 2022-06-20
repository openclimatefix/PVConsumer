""" Make Solar shieffled passiv data file"""

from pvconsumer.solar_sheffield_passiv import (
    get_all_latest_pv_yield_from_solar_sheffield,
)

# get data
pv_yields = get_all_latest_pv_yield_from_solar_sheffield()

# format
pv_yields['provider'] = 'solar_sheffield_passiv'
pv_yields.rename(columns={'system_id':'pv_system_id'}, inplace=True)
pv_yields.rename(columns={'kWp':'installed_capacity_kw'}, inplace=True)

# save
pv_yields[['ss_id', 'pv_system_id', 'provider','installed_capacity_kw']].to_csv('./Documents/Github/PVConsumer/pv_systems_solar_sheffield_passiv.csv')