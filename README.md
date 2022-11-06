# db-to-dwh

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