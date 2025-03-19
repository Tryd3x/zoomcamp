import argparse
import subprocess

def main(params):
    url = params.url
    csv_name = 'output.csv'

    # download the csv
    try:
        wget_proc = subprocess.run(
            ['wget', '-q', '-O', '-', url],
            stdout=subprocess.PIPE,
            check=True,
            timeout=60,
        )

        with open(f"../datasets/{csv_name}", "wb") as f:
            gunzip_proc = subprocess.Popen(
                ['gunzip'],
                stdin=subprocess.PIPE,
                stdout=f,
            )

            gunzip_proc.communicate(input=wget_proc.stdout)

        print("Download successful!")
    except Exception as e:
        print("Error!")
        print(e)
        exit(99)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test for wget and gunzip")

    parser.add_argument('--url',help="pass url to download file from")
    args = parser.parse_args()
    main(args)