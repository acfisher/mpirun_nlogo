import glob

data_files = glob.glob("./proc*.csv")
data_files.sort()

all_lines = [];
for fname in data_files:
    with open(fname, "r") as fin:
        all_lines += fin.readlines()
all_lines.sort()

with open("headers.dat", "r") as fin:
    headers = fin.readlines()

for header in headers:
    header_exp = header.partition(",")[0]
    for i in range(len(all_lines)):
        line_exp = all_lines[i].partition(",")[0]
        if header_exp == line_exp:
            all_lines.insert(i, header)
            break 

bsnum = 0
bsexp = all_lines[0].partition(",")[0]
fout = open("bs_000.csv", "w")
for line in all_lines:
    lineexp = line.partition(",")[0]
    if lineexp == bsexp:
        fout.write(line)
    else:
        fout.close()
        bsnum += 1
        bsexp = lineexp
        fout = open("bs_" + str(bsnum).zfill(3) + ".csv", "w")
        fout.write(line)
fout.close()
