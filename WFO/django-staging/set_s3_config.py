import os
import shutil

DATA_CENTER = os.getenv('DATA_CENTER')
DEFAULT_S3_CONFIG_TARGET_PATH = '/root/.s3cfg'

try:
    os.remove(DEFAULT_S3_CONFIG_TARGET_PATH)
    print(f' File {DEFAULT_S3_CONFIG_TARGET_PATH} deleted')
except FileNotFoundError:
    print(f' File {DEFAULT_S3_CONFIG_TARGET_PATH} not found')

if DATA_CENTER == 'asia':
    src_path = '/app/s3config/bangalore.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
elif DATA_CENTER == 'americas':
    src_path = '/app/s3config/fp.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
elif DATA_CENTER == 'europe':
    src_path = '/app/s3config/espoo.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
elif DATA_CENTER == 'china':
    src_path = '/app/s3config/china.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
elif DATA_CENTER == 'uat':
    src_path = '/app/s3config/uat.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
elif DATA_CENTER == 'dev':
    src_path = '/app/s3config/dev_.s3config'
    shutil.copyfile(src_path, DEFAULT_S3_CONFIG_TARGET_PATH)
else:
    raise ValueError(f'Unknown data center => DATA_CENTER={DATA_CENTER}')
print(f'Config S3 set = {src_path} => {DEFAULT_S3_CONFIG_TARGET_PATH}')

