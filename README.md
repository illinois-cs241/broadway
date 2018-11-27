# broadway-grader

## Starting Grader Instance
- Requires Python 3.5+
- Install the required packages specified in [requirements.txt](requirements.txt) by:
```shell
pip install -r requirements.txt
```
- Install Docker using this [convinience script](https://get.docker.com/)
- Complete [post-installation steps](https://docs.docker.com/install/linux/linux-postinstall/)
- Install Node [guide](https://websiteforstudents.com/install-the-latest-node-js-and-nmp-packages-on-ubuntu-16-04-18-04-lts/)
- Install node packages specified in [package.json](package.json) by:
```shell
npm install
```
- Make sure `SERVER_HOSTNAME` is pointing to the [API](https://github.com/illinois-cs241/broadway-api). Start the [grader](grader.py) by:
```shell
sudo python grader.py <cluster token>
```
