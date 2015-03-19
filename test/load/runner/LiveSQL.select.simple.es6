var _ = require('lodash')
var LiveSQL = require('../../../')
var common = require('../../../src/common')

var liveDb = new LiveSQL(options.conn, options.channel)

liveDb.on('error', function(error) {
	console.error(error)
})

var selectCount = 
	settings.maxSelects && settings.maxSelects < settings.init.classCount ?
		settings.maxSelects : settings.init.classCount

module.exports = _.flatten(_.range(settings.instanceMultiplier || 1)
	.map(instance => _.range(selectCount).map(index => {

	getAssignmentIds(index + 1).then(assignmentIds => {
		var select = liveDb.select(`
			SELECT
				*
			FROM
				scores
			WHERE
				assignment_id IN (${assignmentIds.join(', ')})
			ORDER BY
				id ASC
		`, (diff, rows) => {
			var scoreIds = null
			if(diff.added) {
				scoreIds = diff.added.map(row => {
					return {
						id: row.score_id,
						score: row.score
					}
				})
			}
			process.send({
				type: 'CLASS_UPDATE',
				time: Date.now(),
				classId: index + 1,
				refreshCount: liveDb.refreshCount,
				scores: scoreIds
			})
		})
	})

})))

async function getAssignmentIds(classId) {
	var handle = await common.getClient(options.conn)
	var result = await common.performQuery(handle.client,
		`SELECT id FROM assignments WHERE class_id = $1`, [ classId ])
	handle.done()

	return result.rows.map(row => row.id)
}
