sed -i.bak "s/$(printf '\r')//" _github_setup.sh
git clone https://github.com/thepang/templang.git
mv templang $1
cd $1
rm -rf .git
git init
git add *
git commit -m "First commit"
git remote add origin https://github.com/thepang/$1.git
git push -u origin master

conda create -n $1 python=3.7 
conda install --file requirements.yaml