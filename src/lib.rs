//! Glue code to call Peppi from Julia
//!
//! "Peppi is a Rust parser for .slp game replay files for Super Smash Brothers Melee for the 
//! Nintendo Gamecube. Peppi aims to be the fastest parser for .slp files" - Peppi readme
//!
//! The content of this module is exported to Julia using the [julia_module] macro from [jlrs], or
//! otherwise serves to facilitate interfacing between Peppi and Julia. This code is not
//! distributed as a crate but as a JLL. You can read more about JLLs [here]. The build recipe can
//! be found [in the Yggdrasil repository]. Items exposed by this library can be accessed by using
//! the [Peppi package].
//!
//! [here]: https://docs.binarybuilder.org/stable/
//! [in the Yggdrasil repository]: https://github.com/JuliaPackaging/Yggdrasil/blob/master/R/peppi/build_tarballs.jl
//! [Peppi package]: https://github.com/hohav/peppi

use jlrs::{
    data::managed::{
        ccall_ref::CCallRefRet,
        string::{JuliaString, StringRet},
        value::typed::TypedValue,
    },
    prelude::*,
    weak_handle_unchecked,
};
use arrow2::array::{Array};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use arrow2::datatypes::{Schema, Field};
use arrow2::chunk::Chunk;
use std::{fs, io};

use peppi::frame::PortOccupancy;
use peppi::game::{Start, ICE_CLIMBERS};
use peppi::game::immutable::Game as SlippiGame;
use peppi::io::slippi::de::Opts as SlippiReadOpts;

/// Game data structure exposed to Julia
#[derive(OpaqueType)]
#[jlrs(key = "Game")]
pub struct Game {
    pub start: String,
    pub end: Option<String>,
    pub metadata:Option<String>,
    pub hash: Option<String>,
    pub frames_arrow_path: String, // Path to Arrow IPC file for memory-mapping
}

impl Game {
    /// Get the start data as a Julia String via StringRet
    pub fn get_start(&self) -> StringRet {
        let handle = unsafe { weak_handle_unchecked!() };
        JuliaString::new(handle, &self.start).leak()
    }

    /// Get the end data as a Julia String via StringRet (empty if missing)
    pub fn get_end(&self) -> StringRet {
        let handle = unsafe { weak_handle_unchecked!() };
        let s = self.end.as_deref().unwrap_or("");
        JuliaString::new(handle, s).leak()
    }

    /// Get the metadata as a Julia String via StringRet (empty if missing)
    pub fn get_metadata(&self) -> StringRet {
        let handle = unsafe { weak_handle_unchecked!() };
        let s = self.metadata.as_deref().unwrap_or("");
        JuliaString::new(handle, s).leak()
    }

    /// Get the game hash as a Julia String via StringRet (empty if missing)
    pub fn get_hash(&self) -> StringRet {
        let handle = unsafe { weak_handle_unchecked!() };
        let s = self.hash.as_deref().unwrap_or("");
        JuliaString::new(handle, s).leak()
    }

    /// Get the Arrow IPC file path as a Julia String
    pub fn get_frames_arrow_path(&self) -> StringRet {
        let handle = unsafe { weak_handle_unchecked!() };
        JuliaString::new(handle, &self.frames_arrow_path).leak()
    }
}

pub fn read_slippi(path: JuliaString, skip_frames:i8) -> CCallRefRet<Game> {
    // Open the file and parse the Slippi replay into an immutable Game.
    // JuliaString::as_str returns a Result; avoid `?` by using unchecked.
    let path_str = unsafe { path.as_str_unchecked() };
    let file = fs::File::open(path_str).expect("Failed to open file");

    let mut reader = io::BufReader::new(file);
    // Use default parse options; `parse_opts` is accepted but not yet decoded.
    let opts = SlippiReadOpts{
		skip_frames: skip_frames != 0,
		..Default::default()
	};
    let slippi_game: SlippiGame = peppi::io::slippi::read(&mut reader, Some(&opts))
        .expect("Failed to read Slippi file");

    // Map fields from SlippiGame similar to the PyO3 example.
    let start_json = serde_json::to_string(&slippi_game.start).unwrap_or_default();
    let end_json = slippi_game
        .end
        .as_ref()
        .and_then(|m| serde_json::to_string(m).ok());
    let metadata_json = slippi_game
        .metadata
        .as_ref()
        .and_then(|m| serde_json::to_string(m).ok());

    // Convert frames to Arrow IPC bytes
    let frames_struct_array = slippi_game.frames.into_struct_array(
        slippi_game.start.slippi.version,
        &port_occupancy(&slippi_game.start),
    );

    // Write to Arrow IPC file for memory-mapping
    let schema = Schema::from(vec![Field {
        name: "frame".to_string(),
        data_type: frames_struct_array.data_type().clone(),
        is_nullable: false,
        metadata: Default::default(),
    }]);

    let chunk = Chunk::new(vec![Box::new(frames_struct_array) as Box<dyn Array>]);
    
    // Create a temporary Arrow file - using a deterministic path based on hash or temp dir
    let arrow_path = std::env::temp_dir()
        .join(format!("slippi_frames_{}.arrow", 
            slippi_game.hash.as_deref().unwrap_or("unknown")));
    
    let arrow_file = fs::File::create(&arrow_path)
        .expect("Failed to create Arrow file");
    
    let mut writer = FileWriter::try_new(
        arrow_file,
        schema,
        None,
        WriteOptions { compression: None },
    ).expect("Failed to create Arrow writer");
    
    writer.write(&chunk, None).expect("Failed to write Arrow chunk");
    writer.finish().expect("Failed to finish Arrow writer");

    let arrow_path_str = arrow_path.to_str()
        .expect("Path contains invalid UTF-8")
        .to_string();

	
    // Leak the exported Game to Julia through jlrs.
    let handle = unsafe { weak_handle_unchecked!() };
    CCallRefRet::new(TypedValue::new(
		handle, 
		Game {
			start: start_json,
			end: end_json,
			metadata: metadata_json,
			hash: slippi_game.hash,
			frames_arrow_path: arrow_path_str,
    	}
	).leak())
}

fn port_occupancy(start: &Start) -> Vec<PortOccupancy> {
    start
        .players
        .iter()
        .map(|p| PortOccupancy {
            port: p.port,
            follower: p.character == ICE_CLIMBERS,
        })
        .collect()
}


julia_module! {
    become peppi_jlrs_init;
		
	/// read_slippi_path(path::String)
	///
    /// Read a Slippi replay file from the given path and return a SlippiGame object.
    struct Game;

    fn read_slippi(path: JuliaString, skip_frames: i8) -> CCallRefRet<Game> as read_slippi;

    // Expose getters to Julia
    #[untracked_self]
    in Game fn get_start(&self) -> jlrs::data::managed::string::StringRet as get_start;
    #[untracked_self]
    in Game fn get_end(&self) -> jlrs::data::managed::string::StringRet as get_end;
    #[untracked_self]
    in Game fn get_metadata(&self) -> jlrs::data::managed::string::StringRet as get_metadata;
    #[untracked_self]
    in Game fn get_hash(&self) -> jlrs::data::managed::string::StringRet as get_hash;
    #[untracked_self]
    in Game fn get_frames_arrow_path(&self) -> jlrs::data::managed::string::StringRet as get_frames_arrow_path;
}
