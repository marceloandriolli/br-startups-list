# Crawler Brazillian Startups
Brazilian Startups List


## Dependecies 
- Python 3.9
- Pyenv
- Pyenv-virtualenv

## Setup:

- Install pyenv + pyenv-virtualenv: Follow the instruction in and  https://github.com/pyenv/pyenv and https://github.com/pyenv/pyenv-virtualenv
### Create de env with python 3.9:

```sh
pyenv virtualenv 3.9 startup-crawler-env
```

### Activate the env:
```sh
pyenv activate startup-crawler-env
```

### Install requeriments:
```sh
pip install -r requeriments/txt
```

## Run crawler:
```sh
python crawler.py
```