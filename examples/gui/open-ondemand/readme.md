# GUFI GUI

## Summary
The purpose of this is to show the basic method for integrating GUFI with ondemand.  This is just a simple GUI using the GUFI ls, stat, and find scripts.   Using with Open OnDemand as detailed here will provide authentication.

## Prereqs

- Open OnDemand installed with authentiction provider configured.
- GUFI is installed on the system and configured /etc/GUFI/config
- Ability to install python packages (flask).

## Run without on demand

```
cd ~/GUFI/examples/gui/open-ondemand
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py
```

## Ondemand development

### enable for dev
```
sudo mkdir -p /var/www/ood/apps/dev/${USER}
cd /var/www/ood/apps/dev/${USER}
sudo ln -s ~/ondemand/dev gateway
cp -r ~/GUFI/examples/gui/open-ondemand ~/ondemand/dev
```

To run via ondemand:
1. Login to Open OnDemand
2. Click "develop" from the menu bar and "my sandbox apps"
3. Click "Launch GUFI"

## Install for all users

**Note** ondemand expectation is that it is that the web app itself is a git repo or it throws an error, so setup git repo first

```
cd /var/www/ood/apps/sys
sudo git clone ~/ondemand/dev/gufi-gui
```
