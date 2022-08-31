#!/usr/bin/env bash

# Set colors
GREEN='\033[1;32m'
CYAN='\033[1;36m'
NC='\033[0m'

printf "${GREEN}----${NC}"

echo

# Set user email
git config --local user.email "services@carrusel_data_downloads.com"
printf "${CYAN}User email 'services@carrusel_data_downloads.com' was set.${NC}"

echo
echo

# Set user name
git config --local user.name "carrusel_data_downloads"
printf "${CYAN}User name 'carrusel_data_downloads' was set.${NC}"
echo
printf "${GREEN}----${NC}"
