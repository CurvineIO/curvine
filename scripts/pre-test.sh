cd /root/codespace/barry/codespace/curvine-dailytest/curvine
git restore .
git pull
patch -p1 < /root/codespace/barry/codespace/curvine-dailytest/1.diff
make all