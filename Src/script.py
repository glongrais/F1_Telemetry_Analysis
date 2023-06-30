import bpy
import json
import pandas as pd

file = '/Users/guillaumelongrais/Documents/Code/Python/F1_Telemetry_Analysis/Resources/lap_times.json'
data = {}
with open(file, 'r') as f:
    data = json.load(f)
df = pd.DataFrame(data["10"]).T

df["DistanceToDriverAhead"] = pd.to_numeric(df["DistanceToDriverAhead"], errors="coerce").fillna(0)

distance = df["DistanceToDriverAhead"].values

obj = bpy.data.objects["Cube"]
bpy.context.scene.render.fps = 24

bpy.context.scene.frame_start = 0
bpy.context.scene.frame_end = len(distance)*4
obj.location.x = distance[0]

# Create keyframes for each frame
for frame, x in enumerate(distance):
    bpy.context.scene.frame_set(frame*4)
    obj.location.x = x
    obj.keyframe_insert(data_path="location", index=-1)

# Play the animation
bpy.ops.screen.animation_play()
