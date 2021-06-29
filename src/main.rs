use anyhow::anyhow;
use anyhow::Result;
use rand::prelude::*;
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};

async fn run_upload(
    client: rusoto_s3::S3Client,
    bucket: String,
    key: String,
    sizes: Vec<usize>,
) -> std::result::Result<(), (Option<String>, anyhow::Error)> {
    // A list of uploaded parts, needed when we complete the upload
    // at the end.
    let mut uploaded_parts = Vec::new();
    let mut part_number = 1;
    // The total number of bytes stored in `buffers`.
    let mut rng = thread_rng();

    let create_request = rusoto_s3::CreateMultipartUploadRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        ..Default::default()
    };
    let upload = client
        .create_multipart_upload(create_request)
        .await
        .map_err(|e| (None, anyhow!("Bucket: {}, key: {}, {}", bucket, key, e)))?;

    let upload_id = upload
        .upload_id
        .ok_or(anyhow!("Unable to extract upload id"))
        .map_err(|e| (None, e))?;

    for this_part_size in sizes {
        let client = client.clone();
        let current_upload_id = upload_id.clone();
        let mut data: Vec<u8> = vec![1; this_part_size];
        rng.fill_bytes(&mut data);

        let req = rusoto_s3::UploadPartRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: current_upload_id,
            content_length: Some(this_part_size as i64),
            part_number,
            body: Some(data.into()),
            ..Default::default()
        };
        let uploaded_part = client.upload_part(req).await.map_err(|e| {
            (
                Some(upload_id.clone()),
                anyhow!("Bucket: {}, key: {}, {}", bucket, key, e),
            )
        })?;

        uploaded_parts.push(rusoto_s3::CompletedPart {
            part_number: Some(part_number),
            e_tag: uploaded_part.e_tag,
        });

        part_number += 1;
    }

    let upload_summary = rusoto_s3::CompletedMultipartUpload {
        parts: Some(uploaded_parts),
    };

    let complete_request = rusoto_s3::CompleteMultipartUploadRequest {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        multipart_upload: Some(upload_summary),
        ..Default::default()
    };

    client
        .complete_multipart_upload(complete_request)
        .await
        .map_err(|e| {
            (
                Some(upload_id),
                anyhow!("Bucket: {}, key: {}, {}", bucket, key, e),
            )
        })?;

    Ok(())
}

use clap::{AppSettings, Clap};

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    bucket: String,
    key: String,
    region: Region,
    upload_sizes: Vec<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    let client = S3Client::new(opts.region.clone());
    let bucket = opts.bucket.clone();
    let key = opts.key.clone();

    let result = run_upload(
        client.clone(),
        bucket.clone(),
        key.clone(),
        opts.upload_sizes.clone(),
    )
    .await;
    match result {
        Ok(_) => Ok(()),
        Err((upload_id, e)) => {
            if let Some(upload_id) = upload_id {
                let cancel_request = rusoto_s3::AbortMultipartUploadRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id,
                    ..Default::default()
                };

                client.abort_multipart_upload(cancel_request).await.ok();
            }
            Err(e)
        }
    }
}
