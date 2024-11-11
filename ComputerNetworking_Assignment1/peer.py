import socket
import threading
import json
import os
import sys
import time
import hashlib
import bencodepy
import argparse
from utils import *
TRACKER_IP ='10.229.54.110'
TRACKER_PORT =5050
MYIP = '10.229.54.110'
class Peer():
    def __init__(self, ip, port:int = 4040, peer_list:set = set(), header_length = 1024,pieces_storage="pieces",\
                  metainfo_storage ="metainfo", output_storage = "output",\
                    tracker_ip = TRACKER_IP,tracker_port:int=TRACKER_PORT) -> None:
        self.tracker_ip = tracker_ip
        self.tracker_port= tracker_port
        self.ip = ip
        self.port = port
        self.id = hashlib.sha1(f"{self.ip}:{self.port}".encode()).hexdigest()
        self.peer_list = peer_list
        self.header_length = header_length
        self.pieces_storage = pieces_storage
        self.metainfo_storage = metainfo_storage
        self.output_storage = output_storage
        self.tit_for_tat=0
        self.file_download_semaphore = threading.Semaphore()

        if not os.path.exists(self.pieces_storage):
            os.makedirs(self.pieces_storage)

        if not os.path.exists(self.metainfo_storage):
            os.makedirs(self.metainfo_storage)

        if not os.path.exists(self.output_storage):
            os.makedirs(self.output_storage)
        self.upload = 0
        self.download = 0
        self.peer_list_semaphore = threading.Semaphore()
        self.download_semaphore = threading.Semaphore()
        self.upload_semaphore = threading.Semaphore()
        self.socket_peer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.socket_peer.bind((self.ip,self.port))
        self.socket_peer.listen(10)
        print(f"[PEER] Socket is binded to {self.port}")
        self.running  = True
    def run(self):
        thread = threading.Thread(target=self.start)
        thread.daemon = True
        thread.start()
        while self.running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.running = False
                message = {
                    "type":"disconnect",
                    "id": self.id,
                    "ip": self.ip,
                    "port":self.port
                }
                tracker_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tracker_connection.connect((self.tracker_ip,self.tracker_port))
                self.send_message(tracker_connection,message)
                print("exit")
                break
    def start(self):
        print("[STARTING] PEER is starting ...")
        while self.running:
           thread = threading.Thread(target=self.handle_peer, args=(self.socket_peer.accept()))
           thread.daemon = True
           print(f"[ACTIVE CONNECTION] {threading.active_count() - 1}")
           thread.start()
    def stop(self):
        self.running=False
    def handle_peer(self, connection, address):
        print(f"[NEW CONNECTION] {address} connected")
        self.add_peer(peer = address)
        message =  self.recieve_message(connection,address)
        response = self.process_message(message)
        self.response_action(connection,address, response),
        connection.close()
        self.remove_peer(address)
    def join_network(self):
        message = {
            "type": "join",
            "id": self.id,
            "ip": self.ip,
            "port": self.port
        }
        tracker_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tracker_connection.connect((self.tracker_ip,self.tracker_port))
        self.send_message(tracker_connection,message)
        response = self.recieve_message(tracker_connection)
        if response.get("error"):
            return
        self.id = response.get("id")
    def process_message(self, mess):
        type_message = mess.get("type")
        if type_message == "findTorrent":
            file_name = mess.get("file_name")
            response = {
                "action": "response findTorrent",
                "id": self.id,
                "ip": self.ip,
                "port": self.port,
                "hit": self.isObtainedPieces(file_name)
            }
            return response
        elif type_message =="getPieces":
            am_choking = self.is_choking()
            am_interested = self.is_interested()
            if not am_choking:
                file_name = mess.get("file_name")
                pieces = get_piece_list_of_file(file_name,self.pieces_storage)
                response = {
                    "id": self.id,
                    "action":"response download pieces",
                    "pieces": pieces
                }
                return response
            else:
                response = {
                    "action":"response download pieces",
                }
                return response
        elif type_message =="downloadPieces":
            pieces = mess.get("pieces")
            chunk_size = mess.get("chunk")
            name = mess.get("file_name")
            response = {
                "id": self.id,
                "action":"upload pieces",
                "pieces_dicrectory": "pieces",
                "file_name":name,
                "pieces": pieces,
                "chunk": chunk_size
            }
            return response
        else:
            pass
    def response_action(self, connection, address, command):
        if command.get("action") == "response findTorrent":
            self.send_message(connection,command)
        if command.get("action") == "response download pieces":
            self.send_message(connection,command)
        if command.get("action") == "upload pieces":
            pieces = command.get("pieces")
            chunk = command.get("chunk")
            name = command.get("file_name")
            self.send_list_pieces(connection,pieces,name,chunk)
    def isObtainedPieces(self,file_path:str):
        file_path = file_path.split(".")
        extension = file_path[1]
        file_name = file_path[0]
        file_name_list = get_files_in_pieces_directory(self.pieces_storage)
        return (file_name in file_name_list)
    def is_choking(self):
        return False
    def is_interested(self):
        return True
    def parse_metainfo(self,metainfo) -> dict:
        message={
            "type": "torrent",
            "metainfo_path": metainfo
        }
        torrent = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        torrent.connect((self.tracker_ip,self.tracker_port))
        self.send_message(torrent, message)
        response = self.recieve_message(torrent)
        if not response:
            print(f"[ERROR] Cannot find metainfo path in tracker")
            return
        response =response.get("torrent_data")
        response =response.encode("utf-8")
        response = bencodepy.decode(response)
        torrent.close()
        return response
        # with open(metainfo,"rb") as torrent:
        #     data = torrent.read()
        #     decoded_torrent = bencodepy.decode(data)
        #     return decoded_torrent
    def add_peer(self, peer: tuple)-> None:
        with self.peer_list_semaphore:
            try:
                print(f"[PEER] Add peer {peer} to list tracking")
                self.peer_list.add(peer)
            except Exception as e:
                print(e)
    def remove_peer(self,peer: tuple)-> None:
        with self.peer_list_semaphore:
            try:
                print(f"[PEER] remove peer {peer} in list tracking")
                self.peer_list.discard(peer)
            except Exception as e:
                print(e)
    def update_download(self, bytes):
        with self.download_semaphore:
            self.download+= bytes
    def update_upload(self, bytes):
        with self.upload_semaphore:
            self.upload+= bytes
    def send_message(self,connection,mess: dict):
        message = json.dumps(mess)
        message = message.encode("utf-8")
        message_length = str(len(message)).encode("utf-8")
        message_length += b' '*(self.header_length - len(message_length))
        connection.sendall(message_length)
        connection.sendall(message)
    def recieve_message(self, connection, address= None) -> dict:
        message_length = connection.recv(self.header_length).decode("utf-8")
        if not message_length:
            return None
        message_length = int(message_length)
        message = b''
        while len(message) < message_length:
            data = connection.recv(min(message_length - len(message), 1024))
            if not data:
                return None
            message += data
        return json.loads(message)
    def send_file_to_tracker(self, connection,file_path,chunk= 1024):
        message = {
            "file_name": file_path
        }
        self.send_message(connection,message)
        total = 0
        with open(file_path,"rb") as item:
            try:
                while True:
                    data = item.read(chunk)
                    if not data:
                        connection.sendall(b'done')
                        break
                    connection.sendall(data)
                    total += len(data)
                    self.update_upload(sys.getsizeof(data))
                if connection.recv(2) == b'ok':
                    print(f"[SEND PIECE] send successfully : {file_path} with {total/1024} kbs")
            except Exception as e:
                print(e)
    def send_file(self, connection,file_path,chunk= 1024):
        message = {
            "file_name": file_path
        }
        self.send_message(connection,message)
        total = 0
        with open(file_path,"rb") as item:
            try:
                while True:
                    data = item.read(chunk)
                    if not data:
                        connection.sendall(b'done')
                        break
                    connection.sendall(data)
                    total += len(data)
                    self.update_upload(sys.getsizeof(data))
                if connection.recv(2) == b'ok':
                    print(f"[SEND PIECE] send successfully : {file_path} with {total/1024} kbs")
            except Exception as e:
                print(e)
    def recieve_file(self,connection, out_path,chunk=1024, test = 0):
        """
        recieve file from other peer which is connected with
        Args:
            connection (socket):connnection to other peer
            out_path (str): file output path
            chunk (int, optional): chunk size. Defaults to 512*1024 (521kB)

        """
        message = self.recieve_message(connection)
        file_name = message.get("file_name")
        print(f"[RECIEVING PIECE]:{file_name}")
        with open(out_path,"wb") as item:
            total = 0
            while True:
                data = connection.recv(chunk)
                if b'done' in data:
                    idx = data.find(b"done")
                    if idx != -1:
                        remain = data[:idx]
                        item.write(remain)
                        total += len(remain)
                    connection.sendall(b"ok")
                    break
                item.write(data)
                total += len(data)
            print(f"recieve: {total} Kbs data" )
    def send_list_pieces(self,connection,pieces_list, file_name,chunk) :
        for piece in pieces_list:
            self.send_file(connection,f"{self.pieces_storage}/{file_name}/{piece}",chunk)
        print(f"[SEND PIECES] Finished")
    def recieve_list_pieces(self,connection,pieces_list, file_name,chunk) :
        for piece in pieces_list:
            self.recieve_file(connection,f"{self.pieces_storage}/{file_name}/{piece}",chunk)
    def get_peer_list_from_tracker(self, metainfo_path, tracker_ip, tracker_port):
        message={
            "type": "torrent",
            "metainfo_path": metainfo_path
        }
        torrent = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        torrent.connect((self.tracker_ip,self.tracker_port))
        self.send_message(torrent, message)
        response = self.recieve_message(torrent)
        if not response:
            print(f"[ERROR] Cannot find metainfo path in tracker")
            return
        response =response.get("torrent_data")
        response =response.encode("utf-8")
        # response = bencodepy.decode(response)
        torrent.close()
        # -----------------------------------------------
        key =  create_hash_key_metainfo_through_tracker(response)
        # key =  create_hash_key_metainfo(response)
        message = {
            "type":"download",
            "metainfo_hash": key,
            "peer_id": self.id,
            "ip": self.ip,
            "port": self.port,
            "event": "started",
        }
        tracker_connection = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        tracker_connection.connect((tracker_ip,tracker_port))
        self.send_message(tracker_connection,message)
        response = self.recieve_message(tracker_connection)
        if not response:
            print(f"[ERROR] Tracker doesnot response")
            return
        peer_list = response.get("peers")
        tracker_connection.close()
        return peer_list
    def get_pieces_from_peers(self,peer_list,file_name,pieces_downloaded):
        piece_hold_by_peers = []
        for peer in peer_list:
            peer_connnection = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            peer_connnection.connect((peer.get("ip"),peer.get("port")))
            message = {
                "type": "getPieces",
                "file_name" : file_name,
                "id":self.id
            }
            self.send_message(peer_connnection,message)
            response = self.recieve_message(peer_connnection)
            pieces_of_peer = response.get("pieces")
            # remove piece has downloaded
            for item in pieces_of_peer:
                if item in pieces_downloaded:
                    pieces_of_peer.remove(item)

            if pieces_of_peer:
                element = {
                    "id":peer.get("id"),
                    "ip":peer.get("ip"),
                    "port":peer.get("port"),
                    "pieces":pieces_of_peer
                }
                piece_hold_by_peers.append(element)
            peer_connnection.close()
        return piece_hold_by_peers
    def plan_to_download(self,piece_hold_by_peers):
        piece_set = set()
        planned_download_per_peer = []
        for item in piece_hold_by_peers:
            pieces = item.get("pieces")
            planned_download_per_peer.append({"size":0,"pieces": pieces})
            piece_set.update(pieces)
        min_size = float('inf')
        pos = 0
        for piece in piece_set:
            min_size = float('inf')
            for idx, item in enumerate(planned_download_per_peer):
                if piece in item.get("pieces"):
                    item.get("pieces").remove(piece)
                    # item["pieces"] =  item.get("pieces").remove(piece)
                    if item.get("size") < min_size:
                        min_size = item.get("size")
                        pos = idx
            planned_download_per_peer[pos]["size"] += 1
            planned_download_per_peer[pos]["pieces"].append(piece)


        for idx, item in enumerate(piece_hold_by_peers):
            item["pieces"] = planned_download_per_peer[idx]["pieces"]

        return piece_hold_by_peers
    def download_pieces(self,address, pieces_list, file_name, chunk = 1024):
        peer_connection =  socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        peer_connection.connect(address)
        message = {
            "type": "downloadPieces",
            "id":self.id,
            "file_name": file_name,
            "pieces":pieces_list,
            "chunk": chunk
        }
        # with self.file_download_semaphore:
        self.send_message(peer_connection, message)
        self.recieve_list_pieces(peer_connection,pieces_list,file_name,chunk)
    ## error handling
    def error_handling(self, file_path,metainfo_path)-> bool:
        message={
            "type": "torrent",
            "metainfo_path": metainfo_path
        }
        torrent = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        torrent.connect((self.tracker_ip,self.tracker_port))
        self.send_message(torrent, message)
        response = self.recieve_message(torrent)
        if not response:
            print(f"[ERROR] Cannot find metainfo path in tracker")
            return
        response =response.get("torrent_data")
        response =response.encode("utf-8")
        response = bencodepy.decode(response)
        pieces_hash2 = [piece.decode('utf-8') for piece in response.get(b"info").get(b"pieces")]
        pieces_hash= calculate_file_pieces_hash(file_path)
        if pieces_hash2== pieces_hash:
            return True
        return False
    def download_file(self,metainfo_path):
        metainfo = self.parse_metainfo(metainfo_path)
        announce = metainfo.get(b"announce").decode('utf-8')
        announce = announce.split(":")
        tracker_ip = announce[0]
        tracker_port = int(announce[1])
        file_path = metainfo.get(b"info").get(b"name").decode('utf-8').split(".")
        file_name = file_path[0]
        extension = file_path[1]
        total_length = metainfo.get(b"info").get(b"length")
        piece_list2 = metainfo.get(b"info").get(b"pieces")
        piece_list=[]
        for i in piece_list2:
            piece_list.append(i.decode('utf-8'))
        for idx ,piece in enumerate(piece_list):
            piece_list[idx] = f"{idx}_{piece}.txt"
        piece_length = metainfo.get(b"info").get(b"piece length")
        pieces_downloaded = []
        if not os.path.exists(f"{self.pieces_storage}/{file_name}"):
            os.makedirs(f"{self.pieces_storage}/{file_name}")
        pieces_downloaded = get_piece_list_of_file(file_name,self.pieces_storage)
        if sorted(pieces_downloaded) == sorted(piece_list):
            print(f"[DOWNLOAD] Peer is containing full pieces of {file_name}")
            merge_file_from_pieces(f"{self.pieces_storage}/{file_name}",f"output/{file_name}.{extension}")
            return
        peer_list = self.get_peer_list_from_tracker(metainfo_path,tracker_ip,tracker_port)
        if not peer_list:
            print(f"[ERROR] No seeder hold pieces of this file")
            return
        piece_hold_by_peers = self.get_pieces_from_peers(peer_list,file_name,pieces_downloaded)
        piece_hold_by_peers = self.plan_to_download(piece_hold_by_peers)
        if not piece_hold_by_peers:
            print(f"[DOWNLOAD] Dont get full piece of {file_name} because all peer are not get enough piece ")
            return
        for peer in piece_hold_by_peers:
            self.download_pieces((peer.get("ip"),peer.get("port")),peer.get("pieces"),file_name)
        pieces_downloaded = get_piece_list_of_file(file_name,self.pieces_storage)
        if sorted(pieces_downloaded) == sorted(piece_list):
            print(f"[DOWNLOAD] Get full piece of {file_name} ")
            merge_file_from_pieces(f"{self.pieces_storage}/{file_name}",f"{self.output_storage}/{file_name}.{extension}")
            if self.error_handling(f"{self.output_storage}/{file_name}.{extension}",metainfo_path):
                print(f"[ERROR_HANDLING] {file_name} match origin file")
            else:
                print(f"[ERROR_HANDLING] {file_name} does not match origin file")
        else:
            print(f"[DOWNLOAD] Dont get full piece of {file_name} therefore cannot merge file")
        print("_______________________DONE___________________________________")
    def download_files(self, metainfo_path_list):#dùng để torrent download
        if isinstance(metainfo_path_list,str):
            if os.path.isdir(metainfo_path_list):
                print(f"[DOWNLOAD]input folder: {metainfo_path_list}")
                file_name_list = os.listdir(metainfo_path_list)
                file_name_list =  [metainfo_path_list+"/"+ x for x in file_name_list]
                metainfo_path_list = file_name_list
            else:
                metainfo_path_list = [metainfo_path_list]
        thread_list = []
        for metainfo_file in metainfo_path_list:
            thread = threading.Thread(target=self.download_file, args=(metainfo_file,))
            thread_list.append(thread)
            thread.start()
        for thread in thread_list:
            thread.join()
    def upload_request(self,metainfo_hash,metainfo_name):
        request = {
            "type":"upload",
            "id": self.id,
            "ip": self.ip,
            "port": self.port,
            "upload": self.upload,
            "download":self.download,
            "metainfo_hash": metainfo_hash,
            "metainfo_name": metainfo_name
        }
        tracker_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tracker_connection.connect((self.tracker_ip,self.tracker_port))
        self.send_message(tracker_connection,request)
        response = self.recieve_message(tracker_connection)
        if not response.get("hit"):
            print(f"[UPLOAD] Sending {metainfo_name} to tracker")
            self.send_file_to_tracker(tracker_connection,f"{self.metainfo_storage}/{metainfo_name}",chunk=1024)
        tracker_connection.close()
    def upload_torrent(self,file_share,tracker_address):
        create_pieces_directory(file_share,self.pieces_storage)
        file_name = os.path.basename(file_share)
        file_name = file_name.split(".")[0] + ".torrent"
        output_path = f"{self.metainfo_storage}/{file_name}"
        metainfo_hash = create_torrent(file_share, f"{tracker_address[0]}:{tracker_address[1]}",output_path)
        self.upload_request(metainfo_hash,file_name)
    def upload_files(self,file_share_list,tracker_address):# dùng để upload file
        if isinstance(file_share_list,str):
            if os.path.isdir(file_share_list):
                print(f"[UPLOAD] input folder: {file_share_list}")
                file_name_list = os.listdir(file_share_list)
                file_name_list =  [file_share_list+"/"+ x for x in file_name_list]
                file_share_list = file_name_list
            else:
                file_share_list = [file_share_list]
        thread_list = []
        for file_share in file_share_list:
            thread = threading.Thread(target=self.upload_torrent, args=(file_share,tracker_address))
            thread_list.append(thread)
            thread.start()
        for thread in thread_list:
            thread.join()
#####################################################
def upload_peer_test():
    peer = Peer(ip=MYIP,port = 4041,pieces_storage="pieces1",metainfo_storage="metainfo1")
    list_up = ["uploads/2022_MT.pdf"]
    peer.upload_files(list_up,(peer.tracker_ip,peer.tracker_port))
    peer.run()

def download_peer_test():
    peer = Peer(ip=MYIP,port = 4042,pieces_storage="pieces2")
    list_down = ["metainfo/sontung.torrent"]
    peer.download_files(list_down)
################################################################################################
if __name__ == "__main__":
    upload_peer_test()
    # download_peer_test()
