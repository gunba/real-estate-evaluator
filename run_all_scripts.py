import subprocess

def run_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f'Successfully ran {script_path}')
    except subprocess.CalledProcessError as e:
        print(f'Error running {script_path}: {e}')

def main():
    scripts = [
        'abs/get_census_data.py',
        'mesh/get_mesh_block_data.py',
        'osm/get_osm_data.py',
        'reiwa/get_housing_data.py',
        'reiwa/get_property_data.py',
        'scsa/get_school_atar_data.py',
        'wapol/get_crime_data.py',
        'wapol/process_crime_data.py',
        'build_suburb_data.py',
        'build_property_data.py'
    ]

    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
