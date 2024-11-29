import os
import hashlib
# import bencode
import json
###################################################################3
# 512KB piece size
PIECE_LENGTH = 48*1024



def calculate_file_pieces_hash(file_path):
    # Calculate the SHA-1 hash of the file
    sha1 = hashlib.sha1()
    pieces_hash = []
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(PIECE_LENGTH)  # Read in 512KB chunks
            if not data:
                break
            # sha1.update(data)
            sha1 = hashlib.sha1(data).hexdigest()
            pieces_hash.append(sha1)
    return pieces_hash

def create_torrent(file_path, tracker_url, output_file):
    # Calculate file hash
    pieces_hash = calculate_file_pieces_hash(file_path)
    # Calculate file size
    file_size = os.path.getsize(file_path)

    # Generate torrent dictionary
    torrent_data = {
        "announce": tracker_url,
        "info": {
            "name": os.path.basename(file_path),
            "length": file_size,
            "pieces": pieces_hash,
            "piece length": PIECE_LENGTH  # 512KB piece size
        }
    }
    # Encode the torrent dictionary using bencode
    encoded_torrent = bencodepy.encode(torrent_data)
    # # Write the encoded torrent data to a file
    with open(output_file, 'wb') as f:
        # f.write(json.dumps(torrent_data,indent = 4))
        f.write(encoded_torrent)
    return create_hash_key_metainfo(output_file)



################################################################
def write_data_file(data,output_file_name: str):
    # print(str(data))
    with open(output_file_name +".txt", 'wb') as f:
        f.write(data)

# def split_file_to_pieces(file_path:str,output_pieces_directory: str, file_name_index:str):
#     with open(file_path, 'rb') as f:
#         i: int = 0
#         while True:
#             data = f.read(PIECE_LENGTH)  # Read in 512KB chunks
#             if not data:
#                 break
#             write_data_file(data,output_pieces_directory+"/" + file_name_index+"_"+str(i))
#             i+=1

def merge_file_from_pieces(file_paths, output_file_path):
    if os.path.isdir(file_paths):
        file_name_list = os.listdir(file_paths)
        # print(file_name_list)
        file_name_list = sorted(file_name_list,key = lambda x: int(x.split("_")[0]))
        file_paths = [(file_paths+ "/" + file_name) for file_name in file_name_list]
    print(f"[MERGE PIECE] create output file: {output_file_path}")
    with open(output_file_path,"ab") as out:
        try:
            for file_path in file_paths:
                with open(file_path,"rb") as item:
                    data = item.read(PIECE_LENGTH)
                    out.write(data)
        except Exception as e:
            print(e)

###################################
def create_hash_key_metainfo_through_tracker(data):
        # data = data.encode('utf-8')
    # with open(file, "rb") as torrent:
        # data = torrent.read()
        cleaned_data = data.replace(b"\n", b"").replace(b"\r", b"").replace(b" ",b"")
        # print(cleaned_data)
        key = hashlib.sha1(cleaned_data).hexdigest()
        return key

def create_hash_key_metainfo(file):
    with open(file, "rb") as torrent:
        data = torrent.read()
        cleaned_data = data.replace(b"\n", b"").replace(b"\r", b"").replace(b" ",b"")
        key = hashlib.sha1(cleaned_data).hexdigest()
        return key

def create_metainfo_hashtable(directory):
    hash_table = {}
    if not os.path.isdir(directory):
        raise(f"[UTILS] Cannot create hashtable because {directory} is not a directory")

    files_list =  os.listdir(directory)
    if not files_list:
        print(f"[UTILS] Cannot create hashtable because {directory} is emmpty")
        return {}

    for file_name in files_list:
        file_path = directory + "/" + file_name
        key = create_hash_key_metainfo(file_path)
        hash_table.update({key:file_path})
    return hash_table

####################################
def get_files_in_pieces_directory(pieces_root = "pieces"):
    return os.listdir(pieces_root)

def get_idx_and_hash_of_piece(piece_path):
    name = os.path.basename(piece_path)
    name = name.split(".")[0]
    name = name.split("_")
    idx= name[0]
    hash_code = name[1]
    return (idx, hash_code)


def get_piece_list_of_file( filename,pieces_root = "pieces")->list[str]:
    path = f"{pieces_root}/{filename}"
    if os.path.exists(path):
        pieces = os.listdir(path)
        pieces = sorted(pieces, key=lambda file: int(file.split("_")[0]))
        return pieces
    else:
        return []



def create_pieces_directory(file_path,pieces_root = "pieces" ):
    directory_path = os.path.basename(file_path)
    directory_path = directory_path.split(".")[0]
    directory_path = pieces_root+ "/"+ directory_path
    print(f"[CREATING PIECE] in {directory_path}")
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    with open(file_path, 'rb') as f:
        idx: int = 0
        while True:
            # Read in 512KB chunks
            data = f.read(PIECE_LENGTH)
            hex_hash = hashlib.sha1(data).hexdigest()
            hex_hash = str(hex_hash)
            if not data:
                break
            write_data_file(data,f"{directory_path}/{str(idx)}_{hex_hash}")
            idx+=1
# Example usage:
def test_torrent():
    # file_path = 'D:/input_for_computer_network/sd-blob-b01.img'
    file_path = "input/meeting_1.mp4"

    # file_path = 'input/test.txt'

    tracker_url = 'http://localhost:8080'
    output_file = 'output/my_movie.torrent.json'

    create_torrent(file_path, tracker_url, output_file)

import bencodepy

if __name__ == "__main__":
    # test_split_file_to_pieces(file_path,"pieces/movie","mv_part"


    bc = bencodepy.Bencode(
        encoding='utf-8'
    )

    print(bc.decode('d5:title7:Examplee'))
