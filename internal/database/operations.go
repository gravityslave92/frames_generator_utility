package database

const createFunctionSQL = `
	create or replace function most_frequent(arr varchar[]) returns varchar as
	$$
	DECLARE
	    ret varchar;
	BEGIN
	    select *
	    from unnest(arr)
	    group by 1
	    order by count(*) desc
	    limit 1
	    into ret;
	    Return ret;
	
	end;
	$$
	    language plpgsql immutable;
`

const hardFramesGenerationSQL = `
	WITH signs AS (
	    SELECT rs.id, rs.geometry, rs.frame_kind, rd.code
	    FROM ri_signs rs
	             INNER JOIN ri_definitions rd ON rs.definition_id = rd.id
	    WHERE rs.status = '03'
	),
	     frames AS (
	         SELECT geometry, most_frequent(array_agg(frame_kind)) frame_kind, array_agg(id) sign_ids, count(*) total
	         FROM signs
	         GROUP BY geometry
	         HAVING count(id) > 1
	     )
	INSERT
	INTO ri_frames(geometry, kind)
	SELECT geometry, case when frame_kind is null then '0' else frame_kind end AS kind
	FROM frames;
`

// Generate "hard" frames from existing signs.
func GenerateHardFrames() error {
	tx := connPool.MustBegin()
	tx.MustExec("TRUNCATE ri_frames RESTART IDENTITY")
	tx.MustExec("TRUNCATE ri_sign_frame_arrangements RESTART IDENTITY")
	tx.MustExec(createFunctionSQL)
	tx.MustExec(hardFramesGenerationSQL)
	if errCommit := tx.Commit(); errCommit != nil {
		return errCommit
	}

	return nil
}
