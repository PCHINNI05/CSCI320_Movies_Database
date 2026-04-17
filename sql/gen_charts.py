import pandas as pd
import matplotlib.pyplot as plt
import os

print("=== Poster Chart Generator ===\n")

# --- Prompt for file paths ---
fig1_path = input("Path to Fig 1 CSV (runtime groups): ").strip().strip('"')
fig2_path = input("Path to Fig 2 CSV (era + genre):    ").strip().strip('"')
out_dir   = input("Output folder for saved PNGs:        ").strip().strip('"')

if not os.path.isdir(out_dir):
    os.makedirs(out_dir)

# --- Fig 1: Average Rating by Runtime Group ---
df1 = pd.read_csv(fig1_path)

# Enforce a sensible runtime order regardless of CSV row order
order = ['<90', '90-109', '110-129', '130-149', '150+']
df1['runtime_group'] = pd.Categorical(df1['runtime_group'], categories=order, ordered=True)
df1 = df1.sort_values('runtime_group')

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.bar(df1['runtime_group'], df1['avg_rating'], color='#E57373', width=0.55)
ax.set_title('Fig 1. Average Rating by Runtime Group', fontsize=13, fontweight='bold')
ax.set_xlabel('Runtime (minutes)')
ax.set_ylabel('Average Rating')
ax.set_ylim(0, 4.5)
for bar, val in zip(bars, df1['avg_rating']):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
            f'{val:.2f}', ha='center', fontsize=10, fontweight='bold')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
out1 = os.path.join(out_dir, 'fig1_runtime.png')
plt.savefig(out1, dpi=150)
print(f"\nSaved: {out1}")

# --- Fig 2: Rating Comparison by Era and Genre ---
df2 = pd.read_csv(fig2_path)

genre_groups = df2['genre_group'].unique().tolist()
x = range(len(genre_groups))
width = 0.35

older = [df2[(df2['genre_group'] == g) & (df2['era'] == 'Older')]['avg_rating'].values[0] for g in genre_groups]
newer = [df2[(df2['genre_group'] == g) & (df2['era'] == 'Newer')]['avg_rating'].values[0] for g in genre_groups]

fig, ax = plt.subplots(figsize=(7, 5))
b1 = ax.bar([i - width / 2 for i in x], older, width, label='Older (pre-2000)', color='#F48FB1')
b2 = ax.bar([i + width / 2 for i in x], newer, width, label='Newer (2000+)',    color='#81D4FA')
ax.set_title('Fig 2. Rating Comparison by Era and Genre', fontsize=13, fontweight='bold')
ax.set_ylabel('Average Rating')
ax.set_xticks(list(x))
ax.set_xticklabels(genre_groups)
ax.set_ylim(0, 4.5)
ax.legend()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
for bar in list(b1) + list(b2):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
            f'{bar.get_height():.2f}', ha='center', fontsize=10, fontweight='bold')
plt.tight_layout()
out2 = os.path.join(out_dir, 'fig2_era_genre.png')
plt.savefig(out2, dpi=150)
print(f"Saved: {out2}")

# --- Show both ---
plt.show()
print("\nDone.")
