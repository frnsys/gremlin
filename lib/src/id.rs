//! Generate random, human-readable/pronounceable ids,
//! or lighter weight incrementing integer ids.

use std::sync::LazyLock;

use petname::Petnames;

/// Merge lines from multiple files.
macro_rules! join_contents {
    ( $subdir:literal, { $($fname:literal),* $(,)? } ) => {
        concat!(
            $(include_str!(concat!("../assets/words/", $subdir, "/", $fname)), "\n",)*
        )
    }
}

const ADJECTIVES: &str = join_contents!("adjectives", {
    "appearance.txt",
    "colors.txt",
    "food.txt",
    "materials.txt",
    "character.txt",
    "sound.txt",
    "taste.txt",
    "music_theory.txt",
    "physics.txt",
    "speed.txt",
});

const NOUNS: &str = join_contents!("nouns", {
    "astronomy.txt",
    "birds.txt",
    "cheese.txt",
    "chemistry.txt",
    "fish.txt",
    "food.txt",
    "fruit.txt",
    "metals.txt",
    "minerals.txt",
    "plants.txt",
    "seasonings.txt",
    "water.txt",
    "wine.txt",
    "wood.txt",
    "landforms.txt",
    "geography.txt",
});

static IDS: LazyLock<Petnames<'static>> =
    LazyLock::new(|| Petnames::init(ADJECTIVES, "", NOUNS));

/// Generate a random human-readable id.
pub fn generate_id() -> String {
    let mut rng = rand::thread_rng();
    IDS.generate(&mut rng, 2, "-")
}
