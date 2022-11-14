# Transfer by Artie
[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml) [![ELv2](https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg)](https://github.com/artie-labs/transfer/LICENSE.txt)


## Architecture

## Set up

## Build 

## Release

```
docker build .
docker tag IMAGE_ID artielabs/transfer:0.1
docker push artielabs/transfer:0.1
```

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
