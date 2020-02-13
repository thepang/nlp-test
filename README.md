# HI WELCOME TO THE REPO.
NOTES: 
* Run all terminal commands from the root of the repo.
* Download the engage.sh file to get your GitHub setup without needing the go through septs 1-10 of the GitHub section. Don't forget to pass in the name of the GitHub repo as the only argument. `./_github_setup.sh <NAMEOFREPO>`. If there is a permission error, run 'Chmod +x _github_setup.sh`.

## Setup your GitHub and repo
0. Create GitHub repo 
1. Clone repo by using `$ git clone https://github.com/thepang/templang.git` 
2. Rename the folder templang. `$ mv templang/ <NEWNAME>/`
3. Delete the .git folder: `$ rm -r .git`
4. Test with `$ git status` and this should fail cause its not a repo right now.
5. Make a new git repo with `$ git init` which turns current folder into a git repo and it should create a new .git folder for you.
6. Run `$ git add *` to add all the boilerplate stuff that we got from templang that git doesn't know about yet.
7. Commit all that stuff: `$ git commit -m "First commit"`
8. Run `$ git remote add origin https://github.com/thepang/<NEWNAMEHERE>.git` to connect your new local repo to the origin (GitHub).
9. Push all that stuff you added earlier: `$ git push -u origin master`
10. You can now see the new repo in GitHub. Yay. And all the boilerplate stuff is there too.

## Setup your conda environment
1. Create a conda environment with python v. 3.7: `$ conda create -n $NEWNAMEHERE python=3.7`
2. Activate `$ conda activate $NEWNAMEHERE`
3. Congratulations you are now in the new environment. But we are not done yet.
4. Install all the stuff from the requirements.txt by running: `$ conda install --file requirements.txt`
5. Done.

## Other first time setup stuff
1. Turn pre-commit on (see below).

## Other notes:
Boilerplate files that have been included for your enjoyment.
- .flake8:
	- Flake 8 is a linter.
	- It already includes some rules so Flake 8 is lenient for some small things.
- .pre-commit-config.yaml 
	- Makes sure your code is nice before you finalize commit. 
	- It runs flake 8, black, and something that rearranges imports.
	- You need to turn on/off pre-commit. Use these commands:
		- `$ pre-commit install`
		- `$ pre-commit uninstall`
	- To leave pre-commit on, but force a commit through, ignoring pre-commit: `$ git commit --no-verify <rest of command>`