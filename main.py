import socket
import threading
import vintage_story_pb2 as vspb
from google.protobuf.message import DecodeError
import zstandard
from random import randint


"""
packets start with a int size (4 bytes) and are packed to 16 bytes with nulls if they are from the client
"""

client_packets = {
    33: "LoginTokenQuery",
    1: "ClientId",
    2: "ClientIdentification",
    3: "ClientBlockPlaceOrBreak",
    4: "ChatLine",
    5: "ClientRequestJoin",
    6: "ClientPingReply",
    7: "ClientSpecialKey",
    8: "SelectedHotbarSlot",
    9: "ClientLeave",
    10: "ClientServerQuery",
    14: "MoveItemstack",
    15: "FlipItemstacks",
    16: "EntityInteraction",
    18: "PlayerPosition",
    19: "ActivateInventorySlot",
    20: "CreateItemstack",
    21: "PlayerMode",
    22: "MoveKeyChange",
    23: "BlockEntityPacket",
    31: "EntityPacket",
    24: "CustomPacket",
    25: "ClientHandInteraction",
    26: "ToolMode",
    27: "BlockDamage",
    28: "ClientPlaying",
    30: "InvOpenClose",
    32: "RuntimeSetting"
}

server_packets = {
    90: "ServerId",
    77: "LoginTokenAnswer",
    1: "ServerIdentification",
    2: "ServerLevelInitialize",
    3: "ServerLevelProgress",
    4: "ServerLevelFinalize",
    5: "ServerSetBlock",
    7: "ChatLine",
    8: "ServerDisconnectPlayer",
    9: "ServerChunks",
    10: "UnloadServerChunk",
    11: "ServerCalendar",
    15: "ServerMapChunk",
    16: "ServerPing",
    17: "ServerPlayerPing",
    18: "ServerSound",
    19: "ServerAssets",
    21: "WorldMetaData",
    28: "ServerQueryAnswer",
    29: "ServerRedirect",
    30: "InventoryContents",
    31: "InventoryUpdate",
    32: "InventoryDoubleUpdate",
    34: "Entity",
    35: "EntitySpawn",
    36: "EntityDespawn",
    37: "EntityMoved",
    38: "EntityAttributes",
    39: "EntityAttributeUpdate",
    67: "EntityPacket",
    40: "Entities",
    41: "PlayerData",
    42: "MapRegion",
    44: "BlockEntityMessage",
    45: "PlayerDeath",
    46: "PlayerMode",
    47: "ServerSetBlocks",
    48: "BlockEntities",
    49: "PlayerGroups",
    50: "PlayerGroup",
    51: "EntityPosition",
    52: "HighlightBlocks",
    53: "SelectedHotbarSlot",
    55: "CustomPacket",
    56: "NetworkChannels",
    57: "GotoGroup",
    58: "ServerExchangeBlock",
    59: "BulkEntityAttributes",
    60: "SpawnParticles",
    61: "BulkEntityDebugAttributes",
    62: "ServerSetBlocks",
    64: "BlockDamage",
    65: "Ambient",
    66: "NotifySlot",
    68: "IngameError",
    69: "IngameDiscovery",
    70: "ServerSetBlocks",
    71: "ServerSetDecors",
    72: "RemoveBlockLight",
    73: "ServerReady",
    74: "UnloadMapRegion",
    75: "LandClaims",
    76: "Roles"
}

global log_file
log_file = f"{randint(1, 100000000)}.log"

global server_ded
server_ded = False

global client_ded
client_ded = False

def write_log(data):
    with open(log_file, "a") as f:
        f.write(data)

def try_recv(socket, size):
    data = b''

    while len(data) < size:
        #write_log(f"waiting for {size - len(data)}")
        data += socket.recv(size - len(data))

    return data

def from_server(client, c):
    while True:
        if client_ded:
            break

        size_bytes = client.recv(4)
        size = int.from_bytes(size_bytes)
        compressed = False

        if size >> 31:
            write_log("packet is compressed")
            compressed = True

        size &= 0x7fffffff

        if size > 2147483648:
            write_log(f"server packet too large: {size} > 2147483648 (by {size - 2147483648} bytes")
            break


        if size == 0:
            write_log("SERVER DIED")
            break

        write_log(f"from server: {size} {size_bytes}")

        data = try_recv(client, size)

        try:
            if compressed:
                s_packet = vspb.Server().FromString(zstandard.decompress(data))
            else:
                s_packet = vspb.Server().FromString(data)
        except DecodeError:
            write_log(f"\r{data}\nError decoding data (server)")
            break

        if s_packet.id not in server_packets:
            write_log(f"\rUNKNOWN\n{s_packet}\n")
            pass
        else:
            write_log(f"\r{server_packets[s_packet.id]}\n{s_packet}\n")
            pass

        c.send(size_bytes + data)

    server_ded = True

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 42420))

serve = socket.create_server(("127.0.0.1", 25505))
print("waiting for client to connect")
c, _ = serve.accept()

threading.Thread(target=from_server, args=(client, c)).start()

while True:
    if server_ded:
        break

    size_bytes = c.recv(4)
    size = int.from_bytes(size_bytes)
    compressed = False

    if size >> 31:
        write_log("packet is compressed")
        compressed = True


    size &= 0x7fffffff

    if size > 2147483648:
        write_log(f"client packet too large: {size} > 2147483648 (by {size - 2147483648} bytes")
        break

    if size == 0:
        write_log("CLIENT DIED")
        break

    write_log(f"from client: {size} {size_bytes}")
    data = try_recv(c, size)

    fails = 0

    while fails != 3:
        try:
            if compressed:
                c_packet = vspb.Client().FromString(zstandard.decompress(data.rstrip(b'\x00') + b''*fails))
            else:
                c_packet = vspb.Client().FromString(data.rstrip(b'\x00') + b''*fails)
            fails = 3
        except DecodeError:
            if fails >= 3:
                write_log(f"\r{data}\nError decoding data (client)")
                break
            else:
                write_log("adding nulls...")
                fails += 1




    if c_packet.id not in client_packets:
        write_log(f"\rUNKNOWN\n{c_packet}\n")
        pass
    else:
        write_log(f"\r{client_packets[c_packet.id]}\n{c_packet}\n")
        pass

    client.sendall(size_bytes + data)

client_ded = True

c.close()
client.close()
