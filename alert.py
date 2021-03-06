from alpharius import Email
import argparse


def main():
    parser = argparse.ArgumentParser(description='Alpharius alert.')
    parser.add_argument('--log_file', required=True,
                        help='Error log file of the system.')
    parser.add_argument('--error_code', required=True,
                        help='Error code of the run.')
    args = parser.parse_args()
    Email().send_alert(args.log_file, args.error_code)


if __name__ == '__main__':
    main()
