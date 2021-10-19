#!/usr/bin/env python3

import os
import simplejson

if __name__ == '__main__':

    tests_path = 'tests/resources'

    for file in sorted(os.listdir(tests_path)):
        if not file.endswith('-in-expected.json'):
            continue

        print(f'#[case("{file[:-17]}")]')
        file = os.path.join(tests_path,file)

        with open(file,'r') as f:
            data = simplejson.load(f)

        with open(file,'w') as f:
            simplejson.dump(data,f,sort_keys=True,indent=4,ensure_ascii=False)
