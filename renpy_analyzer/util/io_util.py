import os


def prepare_directories(file):
    """
    Create missing directories and notify the user.
    This is used in all write steps.
    """
    directory = os.path.dirname(file)

    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"@ Created new directory: `{directory}`")