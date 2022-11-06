# Transfer by Artie


## Architecture

## Set up

## Build
[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml)

## Tests


## Disorganized atm

## Installing pre-reqs
```bash
# Installs
brew install direnv
echo 'eval "$(direnv hook bash)"' >> ~/.bash_profile

brew install postgresql
brew install zookeeper
brew install kafka

# Starting svcs
brew services restart postgresql@14
brew services restart zookeeper
brew services restart kafka
```
