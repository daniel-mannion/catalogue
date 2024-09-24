from catalogue import Catalogue
import numpy as np

cat = Catalogue(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo",
                        catalogue_name='scantrackmodels')

blank_entry = cat.getBlankEntry()
print(blank_entry)

segment_length = np.arange(4,20,2)
train_noise = np.arange(0, 0.5,0.1)
path_count = 0

for s in segment_length:
    for t in train_noise:
        blank_entry['train_loss'] = -1
        blank_entry['train_noise'] = t
        blank_entry['segment_length'] = s
        blank_entry['id'] = path_count
        blank_entry['path'] = 'MLSP/Models/%d.ckpt'%path_count
        blank_entry['test_accuracy'] = np.random.normal(0.8, 0.05)
        path_count+=1
        cat.insert(blank_entry)