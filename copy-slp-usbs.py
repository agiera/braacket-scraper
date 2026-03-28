#!/bin/python

import os
import shutil
import argparse


base_paths = [
    "/run/media",  # Linux
    "/Volumes", # macOS (typically /Volumes, but using /run/media for consistency)
]


def find_usb_devices():
    """Find mounted USB devices."""
    usb_devices = []
    if os.name == 'posix':  # Linux/macOS
        base_path = "/run/media"
        if os.path.exists(base_path):
            for user_folder in os.listdir(base_path):
                user_path = os.path.join(base_path, user_folder)
                if os.path.isdir(user_path):
                    try:
                        for device in os.listdir(user_path):
                            usb_devices.append(os.path.join(user_path, device))
                    except PermissionError:
                        print(
                            f"Permission denied to access {user_path}. Skipping.")
                        pass
    elif os.name == 'nt':  # Windows
        for drive_letter in range(68, 91):  # ASCII 'D' to 'Z'
            drive = f"{chr(drive_letter)}:\\"
            if os.path.exists(drive):
                usb_devices.append(drive)

    valid_usb_devices = []
    for usb_path in usb_devices:
        print(f"Found USB device: {usb_path}")

        slippi_path = os.path.join(usb_path, "Slippi")
        if not os.path.exists(slippi_path):
            print(f"Slippi folder not found in {usb_path}. Skipping.")
            continue

        valid_usb_devices.append(usb_path)

    return valid_usb_devices


def copy_usb_contents(usb_devices, destination_path):
    """Copy USB contents."""
    for usb_path in usb_devices:
        if os.path.isdir(usb_path):
            usb_label = os.path.basename(usb_path)
            slippi_path = os.path.join(usb_path, "Slippi")

            if not os.path.exists(slippi_path):
                print(f"Slippi folder not found in {usb_path}. Skipping.")
                continue
            
            # if slippi folder is empty, skip
            if not os.listdir(slippi_path):
                print(f"Slippi folder in {usb_path} is empty. Skipping.")
                continue

            station_destination_path = os.path.join(
                destination_path, usb_label)

            print(
                f"Copying contents of {slippi_path} to '{station_destination_path}'.")
            os.makedirs(station_destination_path, exist_ok=True)
            shutil.copytree(
                slippi_path, station_destination_path, dirs_exist_ok=True)

            print(
                f"Copied contents of {usb_path} to '{station_destination_path}'.")


def remove_usb_contents(usb_devices):
    """Remove USB contents."""
    for usb_path in usb_devices:
        slippi_path = os.path.join(usb_path, "Slippi")
        if os.path.isdir(slippi_path):
            for item in os.listdir(slippi_path):
                item_path = os.path.join(slippi_path, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            print(f"Removed contents of {slippi_path}, but kept the folder.")
        else:
            print(f"USB path {slippi_path} not found.")
    print("USB contents removed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clone USB contents to Google Drive.")
    parser.add_argument("destination_path",
                        help="The google drive path to clone USB contents.")
    args = parser.parse_args()

    usb_devices = find_usb_devices()

    copy_usb_contents(usb_devices, args.destination_path)

    delete_usb = input("Do you want to delete the USB contents? (y/n): ")
    if delete_usb.lower() == "y":
        remove_usb_contents(usb_devices)
        for usb_path in usb_devices:
            if os.name == 'posix':  # Linux/macOS
                os.system(f"umount {usb_path}")
                print(f"Ejected {usb_path}.")
            elif os.name == 'nt':  # Windows
                print(f"Please eject the USB device at {usb_path} manually.")
