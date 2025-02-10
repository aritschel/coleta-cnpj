from extract import run_extraction_job
from load import main as load_main
from transform import main as transform_main


def main():
    """
    Main function to run the extraction, load, and transform jobs.
    """
    print("Starting extraction job...")
    run_extraction_job()
    print("Extraction job completed.")

    print("Starting load job...")
    load_main()
    print("Load job completed.")

    print("Starting transform job...")
    transform_main()
    print("Transform job completed.")


if __name__ == "__main__":
    main()
