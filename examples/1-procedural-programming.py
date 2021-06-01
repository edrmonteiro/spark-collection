log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "Despacito",
        "All the stars"
]

play_count = 0

def count_plays(song_title):
    global play_count
    for song in log_of_songs:
        if song == song_title:
            play_count = play_count + 1
    return play_count

print(count_plays("Despacito"))