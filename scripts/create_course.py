import argparse

parser = argparse.ArgumentParser(
                    prog='CS341 Create Course',
                    description='What the program does',
                    epilog='Text at the bottom of help')

parser.add_argument('-s', "--semester")

args = parser.parse_args()