#!/bin/bash
usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] -c -u -p -{a} -d {-f} {-t} [col...]

Exports specified collections from a MongoDB container to JSON files
and transfers them to the host file system.

Available options:

-h      Print this help and exit.
-c      MongoDB container name.
-u      User name for authentication.
-p      User password for authentication.
-a      Authentication database. Defaults to "admin".
-d      Database to export collections from. 
-f      Path to export collections to. Defaults to current directory.
-q      Prefix for exported files. Defaults to $(date +%F).
-t      Disable gzip compression. Requires gzip to be installed. (default: enabled)
-col    List of collections to export.
EOF
}
# Default values
auth_db="admin"
compress=true
path="."
prefix=$(date +%F)

# Color setup
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

while getopts h:c:u:p:a:d:f:t option; do
    case $option in
        h) usage;
           exit;;
        c) container="$OPTARG";;
        u) user="$OPTARG";;
        p) password="$OPTARG";;
        a) auth_db="$OPTARG";;
        d) db="$OPTARG";;
        f) path="$OPTARG";;
        t) compress=true;;
        *) usage;
           exit 1;;
    esac
done
shift $((OPTIND-1))

ct=${#@} # Number of collections to export
cc=1 # Current collection to export

echo $file_name

for collection in $@; do
    # Set file name
    file_name=${prefix}_${collection}.json

    # Export collection to JSON file
    echo "(${cc}/${ct}) Exporting collection $collection to $container file system..."
    docker exec "$container" mongoexport -u "$user" -p "$password" --authenticationDatabase "$auth_db" --db "$db" --collection "$collection" --out "$file_name"
    if [ $? -eq 0 ]; then
        echo "${GREEN}(${cc}/${ct})Collection $collection successfully exported...${NC}"
    else
        echo "${RED}(${cc}/${ct})Failed to export collection $collection, exiting...${NC}"
        exit 1
    fi
    
    # Transfer JSON file to host file system
    echo "(${cc}/${ct})Transferring "$file_name" to host file system..."
    docker cp "$container":"$file_name" "$path/$file_name"
    if [ $? -eq 0 ]; then
        echo "${GREEN}(${cc}/${ct})Transferred "$file_name" successfully...${NC}"
    else
        echo "${RED}(${cc}/${ct})Failed to transfer "$file_name", exiting...${NC}"
        exit 1
    fi
    
    # Remove JSON file from container
    echo "Removing intermediate file "$file_name" from $container file system..."
    docker exec "$container" rm "$file_name"
    if [ $? -eq 0 ]; then
        echo "${GREEN}(${cc}/${ct})Removel of intermediate "$file_name" successfully...${NC}"
    else
        echo "${RED}(${cc}/${ct})Failed to remove "$file_name", exiting...${NC}"
        exit 1
    fi

    # Compress JSON file with gzip if specified
    if [ $compress = true ]; then
        gzip "$path/$file_name"
        if [ $? -eq 0 ]; then
            echo "${GREEN}(${cc}/${ct})Compressed "$file_name" successfully...${NC}"
        else
            echo "${RED}(${cc}/${ct})Failed to compress "$file_name", exiting...${NC}"
            exit 1
        fi
    else
        echo "${YELLOW}(${cc}/${ct})Skipping compression of "$file_name"...${NC}"
    fi
    ((cc++))
done
