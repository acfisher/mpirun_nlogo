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
            if i > 0:
                all_lines.insert(i, "\n")
            break 

with open("alldata.csv", "w") as fout:
    for line in all_lines:
        fout.write(line)
